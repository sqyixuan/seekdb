delimiter //;
create or replace package pdms.DM_PKG_LBYK_MNG is

  type cursorType is ref cursor;


  procedure RpmImport(strOrderNum in varchar2, retid out number, retmsg out varchar2);


  procedure LBYKImport(strOrderNum in varchar2, strBatchNum in varchar2, retid out number, retmsg out varchar2);


  procedure LBYKDataList(strOrderNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);


  procedure CheckMSISDN(strOrderNum in varchar2, retid out number, retmsg out varchar2);

end DM_PKG_LBYK_MNG;
//

create or replace package body pdms.DM_PKG_LBYK_MNG is

  procedure RpmImport(strOrderNum in varchar2, retid out number, retmsg out varchar2)
  is
      temp_order_count  number := 0;
      temp_batch_count  number := 0;
      temp_cursor     cursorType := null;
      temp_batch_num    number := 0;
      temp_iccid_start  VARCHAR2(20) :='';
      temp_iccid_end    VARCHAR2(20) :='';
      temp_imsi_start   VARCHAR2(15) :='';
      temp_imsi_end   VARCHAR2(15) :='';
      temp_iccid      VARCHAR2(20) :='';
      temp_imsi     VARCHAR2(15) :='';
      temp_order_type   VARCHAR2(100) :='';
      temp_smsp     VARCHAR2(14) :='';
      temp_pin1     VARCHAR2(4) :='';
      temp_pin2     VARCHAR2(4) :='';
      temp_puk1     VARCHAR2(8) :='';
      temp_puk2     VARCHAR2(8) :='';
      temp_kind_id    number := 0;
      temp_flag     number := 0;
  begin

    select order_count, notes into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
    if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_LBYK then
            retid := 7001;
        retmsg  := '该订单不属于两不一快订单，请确认!';
        return;
    end if;
	

    select SUM(BATCH_COUNT) into temp_batch_count from DM_BATCH where ORDER_CODE=strOrderNum;
    if temp_order_count<>temp_batch_count then
            retid := 7002;
        retmsg  := '订单表中的数量与批次表中的数量不相同!';
        return;
    end if;

    open temp_cursor for select BATCH_COUNT, ICCID_START, ICCID_END, IMSI_START, IMSI_END from DM_BATCH where ORDER_CODE=strOrderNum;

    select count(distinct b.SMSP) into temp_flag from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;

    if temp_flag < 1 then
          retid := 7003;
      retmsg  := '找不到订单对应的短消息中心!';
      return;
    elsif temp_flag > 1 then
          retid := 7020;
      retmsg  := '同一订单下的各批次对应的短消息中心不一致!';
      return;
      end if;
    select distinct(b.SMSP) into temp_smsp from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;

    select count(*) into temp_flag from (
      select distinct b.BATCH_REGION, b.HLR_NAME from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum
    );
    if temp_flag <> 1 then
          retid := 7016;
      retmsg  := '该订单下的批次信息不属于同一地市、同一网段下!';
      return;
      end if;

    temp_kind_id := DM_PKG_CONST.KIND_ID;
    temp_pin1 := DM_PKG_CONST.PIN1;

    delete from DM_LBYK_RPM_TEMP;
    commit;

      if temp_cursor %isopen then
        loop
          fetch temp_cursor into temp_batch_num,temp_iccid_start,temp_iccid_end,temp_imsi_start,temp_imsi_end;
          exit when temp_cursor %notfound;

        temp_batch_count := TO_NUMBER(substr(temp_iccid_end,14,7)) - TO_NUMBER(substr(temp_iccid_start,14,7)) +1;
        if temp_batch_num<>temp_batch_count then
              retid := 7007;
          retmsg  := '批次表中的数量与ICCID起止段数量不相同!';
          return;
        end if;
        temp_batch_count := TO_NUMBER(substr(temp_imsi_end,12,4)) - TO_NUMBER(substr(temp_imsi_start,12,4)) +1;
        if temp_batch_num<>temp_batch_count then
              retid := 7008;
          retmsg  := '批次表中的数量与IMSI起止段数量不相同!';
          return;
        end if;

        for i in 0..temp_batch_num-1 loop

          temp_pin2 := floor(DBMS_RANDOM.VALUE(0,9999));
              if temp_pin2 < 0 or temp_pin2 > 9999 then
                retid := 7004;
            retmsg  := '产生PIN2失败!';
            return;
            end if;
            temp_pin2 := 1;

            temp_puk1 := floor(DBMS_RANDOM.VALUE(0,99999999));
              if temp_puk1 < 0 or temp_puk1 > 99999999 then
                retid := 7005;
            retmsg  := '产生PUK1失败!';
            return;
            end if;
            temp_puk1 := 2;

            temp_puk2 := floor(DBMS_RANDOM.VALUE(0,99999999));
              if temp_puk2 < 0 or temp_puk2 > 99999999 then
                retid := 7006;
            retmsg  := '产生PUK2失败!';
            return;
            end if;
            temp_puk2 := 3;
          temp_iccid := substr(temp_iccid_end,1,13) || substr(TO_CHAR(10000000 + TO_NUMBER(substr(temp_iccid_start,14,7)) + i ), 2,7);
          temp_imsi := substr(temp_imsi_start,1,11) || substr(TO_CHAR(10000 + TO_NUMBER(substr(temp_imsi_start,12,4)) + i ), 2,4);
          dbms_output.put_line(666);
          insert into DM_LBYK_RPM_TEMP values(strOrderNum, temp_imsi, temp_iccid, temp_pin1, temp_pin2, temp_puk1, temp_puk2, temp_smsp, temp_kind_id, '', '');
        end loop;
      end loop;
    end if;

    commit;

    retid := 0;
    retmsg  := '';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end RpmImport;

  procedure LBYKImport(strOrderNum in varchar2, strBatchNum in varchar2, retid out number, retmsg out varchar2)
  is
      temp_order_count  number := 0;
      temp_order_type   VARCHAR2(100) :='';
      temp_flag         number := 0;

      temp_order_code   VARCHAR2(9) :='';
      temp_msisdn       VARCHAR2(13) :='';
      temp_cardSN       VARCHAR2(20) :='';
      temp_k            VARCHAR2(32) :='';
      temp_opc          VARCHAR2(32) :='';
      temp_k1           VARCHAR2(32) :='';
      temp_imsi         VARCHAR2(15) :='';
      temp_iccid        VARCHAR2(20) :='';
      temp_pin1         VARCHAR2(4) :='';
      temp_pin2         VARCHAR2(4) :='';
      temp_puk1         VARCHAR2(8) :='';
      temp_puk2         VARCHAR2(8) :='';
      temp_smsp         VARCHAR2(14) :='';
      temp_kind_id      number := 0;
      temp_reserve1     VARCHAR2(11) :='';
      temp_reserve2     VARCHAR2(128) :='';
      temp_reserve3     VARCHAR2(11) :='';
      temp_reserve4     VARCHAR2(128) :='';

      temp_num_cursor   cursorType := null;
      temp_rpm_cursor   cursorType := null;
      temp_blank_cursor cursorType := null;
  begin

    select order_count, notes into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
    if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_LBYK and temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC_LBYK then
            retid := 7009;
        retmsg  := '该订单不属于两不一快订单，请确认!';
        return;
    end if;

    select count(*) into temp_flag from DM_LBYK_MSISDN_TEMP where ORDER_CODE=strOrderNum;
    if temp_order_count<>temp_flag then
            retid := 7010;
        retmsg  := '订单表中的数量与号码表中的数量不相同!';
        return;
    end if;

    select count(*) into temp_flag from DM_BLANK_CARD;
    if temp_order_count<>temp_flag then
            retid := 7011;
        retmsg  := '订单表中的数量与白卡预置数据表中的数量不相同!';
        return;
    end if;

    select count(*) into temp_flag from DM_LBYK_RPM_TEMP;
    if temp_order_count<>temp_flag then
            retid := 7012;
        retmsg  := '订单表中的数量与两不一快个性化数据表中的数量不相同!';
        return;
    end if;

    delete from DM_LBYK_DATA_TEMP;
    commit;

    open temp_blank_cursor for select t1.CARD_SN, t1.K, t1.OPC, t1.K1 from DM_BLANK_CARD t1 where t1.BATCH_NUM=strBatchNum order by t1.CARD_SN asc;
    open temp_rpm_cursor for select t2.ORDER_CODE,t2.IMSI, t2.ICCID, t2.PIN1, t2.PIN2, t2.PUK1, t2.PUK2, t2.SMSP, t2.KIND_ID, t2.RESERVE1, t2.RESERVE2 from DM_LBYK_RPM_TEMP t2 where t2.ORDER_CODE=strOrderNum order by t2.IMSI asc;
    open temp_num_cursor for select t3.MSISDN, t3.RESERVE1, t3.RESERVE2 from DM_LBYK_MSISDN_TEMP t3 where t3.ORDER_CODE=strOrderNum order by t3.MSISDN asc;

    if temp_blank_cursor %isopen then
        loop
          fetch temp_blank_cursor into temp_cardSN,temp_k,temp_opc,temp_k1;
          exit when temp_blank_cursor %notfound;
          fetch temp_rpm_cursor into temp_order_code,temp_imsi,temp_iccid,temp_pin1,temp_pin2,temp_puk1,temp_puk2,temp_smsp,temp_kind_id,temp_reserve1,temp_reserve2;
          exit when temp_rpm_cursor %notfound;
          fetch temp_num_cursor into temp_msisdn,temp_reserve3,temp_reserve4;
          exit when temp_num_cursor %notfound;

          insert into DM_LBYK_DATA_TEMP values(temp_order_code,temp_msisdn,temp_cardSN,temp_k,temp_opc,temp_k1,temp_imsi,temp_iccid,temp_pin1,temp_pin2,temp_puk1,temp_puk2,temp_smsp,temp_kind_id,temp_reserve1,temp_reserve3,temp_reserve2,temp_reserve4);
        end loop;
    end if;
    commit;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end LBYKImport;

  procedure LBYKDataList(strOrderNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)

  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
    temp_order_count  number := 0;
    temp_order_type VARCHAR2(100) :='';
    temp_num number := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select order_count, notes into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
    if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_LBYK and temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC_LBYK then
        retid := 7013;
        retmsg  := '该订单不属于两不一快订单，请确认!';
        return;
    end if;

    select count(*) into temp_num from DM_LBYK_DATA_TEMP where ORDER_CODE = strOrderNum;
    if temp_num < 1 then
      retid := 7014;
      retmsg  := '该订单对应的两不一快数据不存在！';
      return;
    end if;

    if temp_order_count<>temp_num then
            retid := 7015;
        retmsg  := '订单表中的数量与号码表中的数量不相同!';
        return;
    end if;

    open results for
      select * from (
        select t.*, ROWNUM NUM
        from (select * from DM_LBYK_DATA_TEMP where ORDER_CODE = strOrderNum order by CARD_SN) t
      ) where NUM between temp_minPage and temp_maxPage;

    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end LBYKDataList;

  procedure CheckMSISDN(strOrderNum in varchar2, retid out number, retmsg out varchar2)
  is
    temp_region     VARCHAR2(16) := '' ;
    temp_order_count  number := 0;
    temp_order_type   VARCHAR2(100) :='';
    temp_num      number := -1;
    temp_num2       number := -1;
    temp_index      number := 1;
    temp_str      VARCHAR2(1024) :='';
    temp_dataSeg    VARCHAR2(10) :='';
    temp_cursor     cursorType := null;
    temp_attrID     NUMBER(3) :=-1;
    temp_attrName     VARCHAR2(24) := '' ;
    temp_batch_attrName VARCHAR2(24) := '' ;
    temp_msisdn_attrName VARCHAR2(24) := '' ;

    temp_flag         number := 0;
  begin


    select order_count, notes into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
    if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_LBYK and temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC_LBYK then
      retid := 7017;
      retmsg  := '该订单不属于两不一快订单，请确认!';
      return;
    end if;

    select count(*) into temp_num from DM_LBYK_MSISDN_TEMP where ORDER_CODE = strOrderNum;
    if temp_num <> temp_order_count then
      retid := 7018;
      retmsg  := '该订单数量与号码数量不一致！';
      return;
    end if;

    select count(distinct b.BATCH_REGION) into temp_num from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;
    if temp_num < 1 then
      retid := 7019;
      retmsg  := '找不到订单对应的地市信息!';
      return;
    elsif temp_num > 1 then
      retid := 7021;
      retmsg  := '同一订单下的个性化批次对应的地市信息不一致!';
      return;
    end if;

    select distinct(b.BATCH_REGION) into temp_region from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;

    select count(distinct b.HLR_NAME) into temp_num from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;
    if temp_num < 1 then
          retid := 7028;
      retmsg  := '找不到订单对应的HLR_NAME信息!';
      return;
    elsif temp_num > 1 then
          retid := 7029;
      retmsg  := '同一订单下的个性化批次对应的HLR_NAME信息不一致!';
      return;
    end if;

    select distinct(b.HLR_NAME) into temp_batch_attrName from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;


    select count(*) into temp_flag from DM_LBYK_MSISDN_TEMP t  where t.ORDER_CODE = strOrderNum and length(t.msisdn)<>11;
    if temp_flag > 0 then
            retid := 7025;
        retmsg  := '号码长度不正确!';
        return;
    end if;


    select count(*) into temp_flag from DM_LBYK_MSISDN_TEMP t where substr(t.MSISDN,0,7) not in (select DATA_SEG from DM_DATA_SEG);
    if temp_flag > 0 then
            retid := 7026;
        retmsg  := '号段不正确!';
        return;
    end if;

    select count(*) into temp_flag from (select count(t.MSISDN) from DM_LBYK_MSISDN_TEMP t where t.ORDER_CODE = strOrderNum  group by t.MSISDN having count(t.MSISDN) > 1);
    if temp_flag > 0 then
            retid := 7027;
        retmsg  := '检测出重复号码!';
        return;
    end if;

    select count(*) into temp_num from DM_DATA_SEG c,(
      select distinct substr(a.MSISDN, 1, 7) as data_seg from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum) d
      where c.data_seg=d.data_seg;
    select count(distinct substr(a.MSISDN, 1, 7)) into temp_num2 from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum;
    if temp_num<>temp_num2 then

      open temp_cursor for
	  select MSISDN into temp_num2 from(
        select distinct substr(a.MSISDN, 1, 7) as MSISDN from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum
        minus
        select c.data_seg from DM_DATA_SEG c,(
          select distinct substr(a.MSISDN, 1, 7) as data_seg from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum) d
          where c.data_seg=d.data_seg);

        if temp_cursor is not null and temp_cursor %isopen then
          loop
            fetch temp_cursor into temp_dataSeg;
            exit when temp_cursor %notfound or temp_index>10;
          temp_str := temp_str || temp_dataSeg || ', ';
          temp_index := temp_index + 1;
        end loop;
      end if;
          retid := 7022;
      retmsg  := '以下号段在号段表中不存在，' || temp_str || '请确认!';
      return;
    end if;

    select count(distinct c.ATTR_ID)into temp_num from DM_DATA_SEG c,(
      select distinct substr(a.MSISDN, 1, 7) as data_seg from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum) d
      where c.data_seg=d.data_seg;
    if temp_num <> 1 then
          retid := 7023;
      retmsg  := '两不一快号码不在同一地市下的同一网段下!';
      return;
    end if;

    select distinct c.ATTR_ID into temp_attrID from DM_DATA_SEG c,(
      select distinct substr(a.MSISDN, 1, 7) as data_seg from DM_LBYK_MSISDN_TEMP a where a.ORDER_CODE=strOrderNum) d
      where c.data_seg=d.data_seg;
    select ATTR_NAME into temp_attrName from DM_HLR_LINK where ATTR_ID=(select PARENT_ID from DM_HLR_LINK where ATTR_ID=temp_attrID);
    if temp_attrName<>temp_region then
          retid := 7024;
      retmsg  := '两不一快号码所属地市与个性化数据地市信息不匹配!';
      return;
    end if;

    select ATTR_NAME into temp_msisdn_attrName from DM_HLR_LINK where ATTR_ID=temp_attrID;
    if temp_msisdn_attrName<>temp_batch_attrName then
      retid := 7030;
      retmsg  := '临时号与个性化数据不属于同一地市的同一网段!';
      return;
    end if;

    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CheckMSISDN;

end DM_PKG_LBYK_MNG;

//

delimiter ;//