delimiter //;
create or replace package DM_PKG_DATA_MNG is

  type cursorType is ref cursor;

  -- 批次列表查询（2014-7-18 曾照熙-优化）
  procedure BatchInfoList1(strBatchNum in varchar2, iBatchType in number, strDataSeg in varchar2, strImsiStart in varchar2, strRegionName in varchar2, strFactoryCode in varchar2, strCardType in varchar2, iDataStatus in number, iOrderStatus in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  -- 批次列表查询（停用）
  procedure BatchInfoList2(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  -- 获取指定的批次信息
  procedure BatchInfo(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  -- 批次信息删除
  procedure BatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2);

  -- 生成订单号
  procedure OrderNumGenerate(strFactoryCode in varchar2, strOrderNum out varchar2, retid out number, retmsg out varchar2);

  -- 多个批次合成一个生成订单
  procedure OrderInfoAdd(strBatchNums in varchar2, strOrderCode in varchar2, strOrderType in varchar2, retid out number, retmsg out varchar2);

  -- 删除订单
  procedure OrderInfoDel(strOrderCode in varchar2, retid out number, retmsg out varchar2);

  -- 订单列表查询
  procedure OrderInfoList(strOrderType in varchar2, strOrderNum in varchar2, strFactoryCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  -- 需要生成订单的列表
  procedure OrderInfoGenList(strFactoryCode in varchar2,strFactoryName in varchar2, strCardName in varchar2, strStartTime in varchar2, strEndTime in varchar2, records out number, results out cursorType, retid out number, retmsg out varchar2);

  -- 获取指定的订单详细信息的查询
  procedure OrderDetailInfo(strOrderNum in varchar2, results out cursorType, detailInfo out cursorType, retid out number, retmsg out varchar2);

  -- 修改订单信息状态
  procedure OrderStateModify(strOrderNum in varchar2, iStatus in number, retid out number, retmsg out varchar2);

  -- 统计指定状态值的信息数量
  procedure StatusCount(iStatus in number, records out number, retid out number, retmsg out varchar2);

  -- 数据订单批量制作
  procedure OrderImport(strRegionName in varchar2, strCardName in varchar2, strCount in number, strFactoryName in varchar2, strFactoryCode in varchar2, strDataSeg in varchar2, retid out number, retmsg out varchar2);

   -- 标识某批次生成的数据类型
  procedure updateBathDataType(strBatchNum in varchar2,strBatchDataType in varchar2, retid out number, retmsg out varchar2);

  -- 获取某订单状态
  procedure GetOrderStatus(strOrderCode in varchar2,strOrderStatus out varchar2, retid out number, retmsg out varchar2);

end DM_PKG_DATA_MNG;
//

create or replace package body pdms.DM_PKG_DATA_MNG is
	
  procedure BatchInfoList1(strBatchNum in varchar2, iBatchType in number, strDataSeg in varchar2, strImsiStart in varchar2, strRegionName in varchar2, strFactoryCode in varchar2, strCardType in varchar2, iDataStatus in number, iOrderStatus in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
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

		temp_sql_recodes := 'select count(*) as records from DM_BATCH where BATCH_NUM is not null';
		temp_sql_list := 'select * from ( ';
		temp_sql_list := temp_sql_list || 'select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,FACTORY_CODE,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,';
		temp_sql_list := temp_sql_list || 'ICCID_START,ICCID_END,to_char(BATCH_TIME, ''yyyy-mm-dd hh24:mi:ss'') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM ';
		temp_sql_list := temp_sql_list || 'from (select * from DM_BATCH where BATCH_NUM is not null ';

		if strBatchNum is not null then
			temp_sql_recodes := temp_sql_recodes || ' and BATCH_NUM=''' || strBatchNum || '''' ;
			temp_sql_list := temp_sql_list || ' and BATCH_NUM=''' || strBatchNum || '''';
		end if;
		if strDataSeg is not null then
			temp_sql_recodes := temp_sql_recodes || ' and substr(MSISDN_START, 1, 7)=''' || strDataSeg || '''' ;
			temp_sql_list := temp_sql_list || ' and substr(MSISDN_START, 1, 7)=''' || strDataSeg || '''' ;
		end if;
		if strImsiStart is not null then
			temp_sql_recodes := temp_sql_recodes || ' and IMSI_START=''' || strImsiStart || '''' ;
			temp_sql_list := temp_sql_list || ' and IMSI_START=''' || strImsiStart || '''' ;
		end if;
		if strRegionName is not null then
			temp_sql_recodes := temp_sql_recodes || ' and BATCH_REGION=''' || strRegionName || '''' ;
			temp_sql_list := temp_sql_list || ' and BATCH_REGION=''' || strRegionName || '''' ;
		end if;
	if strFactoryCode is not null then
			temp_sql_recodes := temp_sql_recodes || ' and FACTORY_CODE=''' || strFactoryCode || '''' ;
			temp_sql_list := temp_sql_list || ' and FACTORY_CODE=''' || strFactoryCode || '''' ;
		end if;
		if strCardType is not null then
			temp_sql_recodes := temp_sql_recodes || ' and CARD_TYPE=''' || strCardType || '''' ;
			temp_sql_list := temp_sql_list || ' and CARD_TYPE=''' || strCardType || '''' ;
		end if;
		if iDataStatus <> -1 then
			temp_sql_recodes := temp_sql_recodes || ' and PRODUCE_STATUS=' || iDataStatus ;
			temp_sql_list := temp_sql_list || ' and PRODUCE_STATUS=' || iDataStatus ;
		end if;
		if iOrderStatus <> -1 then
			temp_sql_recodes := temp_sql_recodes || ' and BATCH_STATUS=' || iOrderStatus ;
			temp_sql_list := temp_sql_list || ' and BATCH_STATUS=' || iOrderStatus ;
		end if;
		if iBatchType = DM_PKG_CONST.BATCH_TYPE_IMSI_CYC_NO then
			temp_sql_recodes := temp_sql_recodes || ' and (NOTES not like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%'' or NOTES is null)' ;
			temp_sql_list := temp_sql_list || ' and (NOTES not like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%'' or NOTES is null)' ;
		elsif iBatchType = DM_PKG_CONST.BATCH_TYPE_IMSI_CYC_YES then
			temp_sql_recodes := temp_sql_recodes || ' and NOTES like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%''' ;
			temp_sql_list := temp_sql_list || ' and NOTES like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%''' ;
		end if;

		if strStartTime is not null and strEndTime is not null then
			temp_sql_recodes := temp_sql_recodes || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
			temp_sql_list := temp_sql_list || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
		end if;

		temp_sql_list := temp_sql_list || ' order by BATCH_NUM desc)';
		temp_sql_list := temp_sql_list || ') where NUM between ' || temp_minPage || ' and ' || temp_maxPage;

		execute immediate temp_sql_recodes into records;
    open results for temp_sql_list;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg := sqlerrm;
	end BatchInfoList1;


	procedure BatchInfoList2(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
		temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
		temp_minPage number(11) := -1;
		temp_maxPage number(11) := -1;
	begin
		temp_minPage := ((iPage - 1) * iPageSize) + 1;
		temp_maxPage := iPage * iPageSize;

		if strBatchNum is null then
			if strFactoryName is null then
				if strDataSeg is null then
					if strStartTime is null and strEndTime is null then
					-- 所有过滤条件为空时
						select count(*) into records from DM_BATCH;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【时间段】查询
						select count(*) into records from DM_BATCH where BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				else
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【号段】查询
						select count(*) into records from DM_BATCH where substr(MSISDN_START, 1, 7)=strDataSeg;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where substr(MSISDN_START, 1, 7)=strDataSeg order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【号段+时间段】查询
						select count(*) into records from DM_BATCH where substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				end if;
			else
				if strDataSeg is null then
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【时间段+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				else
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【号段+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and substr(MSISDN_START, 1, 7)=strDataSeg;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and substr(MSISDN_START, 1, 7)=strDataSeg order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【号段+时间段+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				end if;
			end if;
		else
			if strFactoryName is null then
				if strDataSeg is null then
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【批次号】查询
						select count(*) into records from DM_BATCH where BATCH_NUM=strBatchNum;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where BATCH_NUM=strBatchNum order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【批次号+时间段】 查询
						select count(*) into records from DM_BATCH where BATCH_NUM=strBatchNum and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where BATCH_NUM=strBatchNum and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				else
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【号段+批次号】查询
						select count(*) into records from DM_BATCH where BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【号段+批次号+时间段】 查询
						select count(*) into records from DM_BATCH where BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				end if;
			else
				if strDataSeg is null then
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【批次号+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【批次号+时间段+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				else
					if strStartTime is null and strEndTime is null then
					-- 过滤条件：按【号段+批次号+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg;
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					else
					-- 过滤条件：按【号段+批次号+时间段+卡商名称】查询
						select count(*) into records from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
						open results for
							select * from (
								select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
									   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
								from (select * from DM_BATCH where FACTORY_NAME=strFactoryName and BATCH_NUM=strBatchNum and substr(MSISDN_START, 1, 7)=strDataSeg and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM desc)
							) where NUM between temp_minPage and temp_maxPage;
					end if;
				end if;
			end if;
		end if;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end BatchInfoList2;

	-----------------------------------------------------------------------------------------
	-- 名称: BatchInfo
	-- 功能: 获取指定的批次信息
	-- 调用: { call DM_PKG_DATA_MNG.BatchInfo(?,?,?,?) }
	-- 参数:
	--	strBatchNum		in	varchar2			批次号
	--	results			out	cursorType			游标类型结果集
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	records			:= 查询出的记录条数
	--	results			:= 游标类型结果集
	--	retid			:= 5001;
	--	retmsg			:= '指定批次[strBatchNum]信息不存在。';
	--	retid			:= 5002;
	--	retmsg			:= '指定批次的HLR信息不存在。';
	--	retid			:= 0;
	--	retmsg			:= '成功!';
	-----------------------------------------------------------------------------------------
	procedure BatchInfo(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
	begin
		select COUNT(*) into temp_flag from DM_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 5001;
			retmsg	:= '指定批次[' || strBatchNum || ']信息不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_BATCH a, DM_HLR b where a.BATCH_NUM=strBatchNum and a.HLR_NAME=b.HLR_NAME;
		if temp_flag < 1 then
			retid	:= 5002;
			retmsg	:= '指定批次的HLR信息不存在。';
			return;
		end if;

		open results for
			select a.BATCH_NUM,a.BATCH_REGION,a.SMSP,a.HLR_NAME,a.BRAND_NAME,a.FACTORY_NAME,a.FACTORY_CODE,a.CARD_TYPE,a.MSISDN_START,a.MSISDN_END,a.DATA_TYPE,a.BATCH_COUNT,a.IMSI_START,a.IMSI_END,
				a.ICCID_START,a.ICCID_END,to_char(a.BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,a.BATCH_STATUS,a.PRODUCE_STATUS,a.ORDER_CODE,a.NOTES, b.HLR_CODE, b.REGION_CODE, ROWNUM NUM
			from DM_BATCH a, DM_HLR b where a.BATCH_NUM=strBatchNum and a.HLR_NAME=b.HLR_NAME;

		update DM_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_GENERATE where BATCH_NUM=strBatchNum;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end BatchInfo;

	-----------------------------------------------------------------------------------------
	-- 名称: BatchInfoDelete
	-- 功能: 删除指定的批次信息
	-- 调用: { call DM_PKG_DATA_MNG.BatchInfoDelete(?,?,?) }
	-- 参数:
	--	strBatchNum		in	varchar2			批次号
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	records			:= 查询出的记录条数
	--	results			:= 游标类型结果集
	--	retid			:= 5003;
	--	retmsg			:= '指定的批次号[strBatchNum]信息不存在！';
	--	retid			:= 5004;
	--	retmsg			:= '删除的批次必须是所属IMSI段最后一个批次！';
	--	retid			:= 5005;
	--	retmsg			:= '删除的批次已经生成订单！';
	--	retid			:= 0;
	--	retmsg			:= '成功!';
	-----------------------------------------------------------------------------------------
	procedure BatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
		temp_imsiSeg varchar2(11) := null;
		temp_imsiEnd varchar2(4) := null;
		temp_max_imsiEnd varchar2(15) := null;
		temp_batchStatus number := -1;
		temp_dataSeg varchar2(7) := null;
		temp_count number := -1;
		temp_batchCount number := -1;
		temp_states number := -1;
		temp_imsi_count number := -1;
	begin
		-- 找不到输入的批次号对应的批次
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM = strBatchNum;
		if temp_flag < 1 then
	        retid	:= 5003;
	        retmsg	:= '指定的批次号[' || strBatchNum || ']信息不存在！';
	        return;
		end if;

		-- 删除的批次必须是所属IMSI段最后一个批次
		select substr(IMSI_END,1,11), substr(IMSI_END,12,4), BATCH_STATUS into temp_imsiSeg, temp_imsiEnd, temp_batchStatus
			from DM_BATCH where BATCH_NUM = strBatchNum;
		select max(substr(IMSI_END,12,4)) into temp_max_imsiEnd from DM_BATCH where substr(IMSI_END,1,11)=temp_imsiSeg;
		if temp_imsiEnd <> temp_max_imsiEnd then
	        retid	:= 5004;
	        retmsg	:= '删除的批次必须是所属IMSI段最后一个批次！';
	        return;
		end if;

		-- 删除的批次已经生成订单
		if temp_batchStatus = DM_PKG_CONST.BACTH_STATE_GENERATE then
	        retid	:= 5005;
	        retmsg	:= '删除的批次已经生成订单！';
	        return;
		end if;

		--查询IMSI表中剩余量
		select IMSI_COUNT into temp_imsi_count from DM_IMSI where IMSI_SEG=temp_imsiSeg;

		--查询批次对应的号段和批次数量
		select substr(MSISDN_START, 1, 7), BATCH_COUNT into temp_dataSeg, temp_batchCount from DM_BATCH where BATCH_NUM = strBatchNum;

		-- 删除批次信息
		delete from DM_BATCH where BATCH_NUM = strBatchNum;

		-- 批次表中查询对应的IMSI段记录数,最大终止IMSI
		select count(*), max(IMSI_END) into temp_flag, temp_max_imsiEnd from DM_BATCH where substr(IMSI_START, 1, 11) = temp_imsiSeg;
		if temp_flag = 0 then
			-- IMSI段未使用
			-- 判断IMSI表中剩余量与批次表中的信息是否一致
			if (temp_imsi_count + temp_batchCount) <> DM_PKG_CONST.IMSI_UNUSED_COUNT then
				retid	:= 5019;
				retmsg	:= 'IMSI表中剩余量与批次表中的信息不一致！';
				return;
			end if;
			-- 先更新IMSI表数据剩余量和状态
			update DM_IMSI set STATUS = DM_PKG_CONST.IMSI_STATE_UNUSED, IMSI_COUNT = DM_PKG_CONST.IMSI_UNUSED_COUNT where IMSI_SEG = temp_imsiSeg;
			select count(*) into temp_flag from DM_BATCH where substr(MSISDN_START, 1, 7) = temp_dataSeg;
			if temp_flag = 0 then
				-- 号段未使用
        if substr(temp_dataSeg, 1, 3) = 195 then
          update DM_DATA_SEG set DATA_SEG_STATE = DM_PKG_CONST.DATA_SEG_STATE_UNUSED, MSISDN_COUNT = DM_PKG_CONST.MSISDN_195_UNUSED_COUNT where DATA_SEG = temp_dataSeg;
        else
				  update DM_DATA_SEG set DATA_SEG_STATE = DM_PKG_CONST.DATA_SEG_STATE_UNUSED, MSISDN_COUNT = DM_PKG_CONST.MSISDN_UNUSED_COUNT where DATA_SEG = temp_dataSeg;
        end if;
			elsif temp_flag > 0 then
				-- 号段已使用
				select MSISDN_COUNT into temp_count from DM_DATA_SEG where DATA_SEG=temp_dataSeg;
				temp_count := temp_count + temp_batchCount;
				update DM_DATA_SEG set DATA_SEG_STATE = DM_PKG_CONST.DATA_SEG_STATE_USED, MSISDN_COUNT = temp_count where DATA_SEG = temp_dataSeg;
			end if;
		elsif temp_flag > 0 then
			-- IMSI段已使用
			-- 判断IMSI表中剩余量与批次表中的信息是否一致
			if (temp_imsi_count + temp_batchCount) <> (10000-to_number(substr(temp_max_imsiEnd, 12, 4))-1) then
				retid	:= 5020;
				retmsg	:= 'IMSI表中剩余量与批次表中的信息不一致！';
				return;
			end if;
			-- 先更新IMSI表数据剩余量和状态
			if substr(temp_max_imsiEnd, 12, 4) = '9999' then
				update DM_IMSI set STATUS = DM_PKG_CONST.IMSI_STATE_ALL_USED,IMSI_COUNT = 0 where IMSI_SEG = temp_imsiSeg;
			else
				update DM_IMSI set STATUS = DM_PKG_CONST.IMSI_STATE_USED,IMSI_COUNT = (10000-to_number(substr(temp_max_imsiEnd, 12, 4))-1) where IMSI_SEG = temp_imsiSeg;
			end if;

			-- 后更新号段表数据剩余量和状态
			select MSISDN_COUNT into temp_count from DM_DATA_SEG where DATA_SEG=temp_dataSeg;
			temp_count := temp_count + temp_batchCount;
			if 0 < temp_count and temp_count <100000 then
				temp_states := DM_PKG_CONST.DATA_SEG_STATE_USED;
			elsif temp_count = 0 then
				temp_states := DM_PKG_CONST.DATA_SEG_STATE_ALL_USED;--应该不会跑到这个分支
			end if;
			update DM_DATA_SEG set MSISDN_COUNT=temp_count, DATA_SEG_STATE=temp_states where DATA_SEG=temp_dataSeg;
		end if;

		-- 判断IMSI表中的剩余量与号段表中的剩余量是否一致
		select MSISDN_COUNT into temp_count from DM_DATA_SEG where DATA_SEG=temp_dataSeg;
		select sum(IMSI_COUNT) into temp_imsi_count from DM_IMSI where DATA_SEG=temp_dataSeg;
		if temp_count<>temp_imsi_count then
			retid	:= 5021;
			retmsg	:= 'IMSI表与号段表的数量不匹配，请确认！';
			rollback;
			return;
		end if;

		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end BatchInfoDelete;

	-----------------------------------------------------------------------------------------
	-- 名称: OrderNumGenerate
	-- 功能: 生成订单号
	-- 调用: { DM_PKG_DATA_MNG.OrderNumGenerate(?,?,?,?) }
	-- 参数:
	--	strFactoryName	in	varchar2			卡商名称
	--	strOrderNum		out	varchar2			订单号
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 5006;
	--	retmsg			:= '卡商[strFactoryName]信息不存在！';
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure OrderNumGenerate(strFactoryCode in varchar2, strOrderNum out varchar2, retid out number, retmsg out varchar2)
	is
	--	temp_factory_code varchar2(2) := null;
		temp_orderNum_1_7 varchar2(7) := null;
		temp_orderNum_8_9 varchar2(2) := null;
		temp_date varchar2(6) :=  to_char(sysdate, 'YYMMDD');
		temp_flag number := -1;
	begin
		-- 检查卡商
	select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = strFactoryCode;
		if temp_flag < 1 then
	     retid	:= 5006;
	     retmsg	:= '卡商不存在！';
	     return;
	  end if;
	-- select FACTORY_CODE into temp_factory_code from DM_FACTORY where FACTORY_NAME = strFactoryName;

		temp_orderNum_1_7 := strFactoryCode || temp_date;

		-- 从批次表中查询匹配的订单号数量和最大值
		select COUNT(*), MAX(SUBSTR(ORDER_CODE,8,2)) into temp_flag, temp_orderNum_8_9 from DM_BATCH where ORDER_CODE like temp_orderNum_1_7 || '%';

		-- 根据批次表中的数据生成新的批次号
		if temp_orderNum_8_9 is null then
			temp_orderNum_8_9 := '00';
		else
			temp_orderNum_8_9 := SUBSTR(to_char(100 + to_number(temp_orderNum_8_9) + 1),2,2);
		end if;
		strOrderNum := temp_orderNum_1_7 || temp_orderNum_8_9;

		retid	:= 0;
		retmsg	:= '成功！';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end OrderNumGenerate;

	-----------------------------------------------------------------------------------------
	-- 名称: OrderInfoAdd
	-- 功能: 多个批次合成一个生成订单
	-- 调用: { call DM_PKG_DATA_MNG.OrderInfoAdd(?,?,?,?) }
	-- 参数:
	--	strBatchNums	in	varchar2		批次号
	--	strOrderCode	in	varchar2		订单号
	--  strOrderType  in  varchar2    订单类型：1-现场写卡， 2-两不一快
	--	retid			out	number			成功/失败信息ID
	--	retmsg			out	varchar2		成功/失败信息描述
	-- 返回值:
	--	retid			:= 5007;
	--	retmsg			:= '输入的卡商订单号在订单信息表中已经存在!';
	--	retid			:= 5008;
	--	retmsg			:= '输入的卡商批次号在批次表中不存在!';
	--	retid			:= 5009;
	--	retmsg			:= '该批次已经生成相应的订单!';
	--	retid			:= 5010;
	--	retmsg			:= '生成订单批次的卡商不一致!';
	--	retid			:= 5011;
	--	retmsg			:= '生成订单批次的卡类型不一致!';
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure OrderInfoAdd(strBatchNums in varchar2, strOrderCode in varchar2, strOrderType in varchar2, retid out number, retmsg out varchar2)
	is
		temp_flag number := 0;
		temp_factoryName varchar2(20);
    temp_factoryCode varchar2(5);
		temp_cardType varchar2(40);
		temp_count number := 0;
		temp_src varchar2(32767) := strBatchNums;
		temp_strBatchNum varchar2(12) := null;
		temp_next varchar2(32767) := ' ';
		temp_state number := -1;
	begin
		-- 判断输入的订单号在订单信息表中是否存在
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE = strOrderCode;
		if temp_flag > 0 then
			retid	:= 5007;
	        retmsg	:= '输入的卡商订单号在订单信息表中已经存在!';
	        return;
        end if;

		-- 循环判断及更新批次表信息
		while (temp_next is not null) loop
			DM_PKG_UTILS.tokenizer(1, ',', temp_src, temp_strBatchNum, temp_next);
			temp_src := temp_next;

			-- 判断输入的批次号在批次表中是否存在
			select count(*) into temp_flag from DM_BATCH where BATCH_NUM = temp_strBatchNum;
			if temp_flag < 1 then
				retid	:= 5008;
		        retmsg	:= '输入的卡商批次号在批次表中不存在!';
		        return;
	        end if;

			-- 判断该批次是否已经生成相应的订单
			select BATCH_STATUS into temp_state from DM_BATCH where BATCH_NUM = temp_strBatchNum;
		        if temp_state <> DM_PKG_CONST.DATA_STATE_INIT then
		        	retid	:= 5009;
		        	retmsg	:= '该批次已经生成相应的订单!';
		        	return;
			end if;

			-- 更新批次表状态和订单号
			update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.BACTH_STATE_GENERATE, ORDER_CODE=strOrderCode where BATCH_NUM = temp_strBatchNum;
			commit;
		end loop;

        -- 判断输入批次厂商是否一致
        select count(distinct(FACTORY_NAME)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderCode;
        if temp_flag <> 1 then
					retid	:= 5010;
	        retmsg	:= '生成订单批次的卡商不一致!';
	        -- 更新批次表状态和订单号
					update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT, ORDER_CODE='0' where ORDER_CODE = strOrderCode;
					commit;
	        return;
        end if;

        -- 判断输入批次卡类型是否一致
        select count(distinct(CARD_TYPE)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderCode;
        if temp_flag <> 1 then
					retid	:= 5011;
	        retmsg	:= '生成订单批次的卡类型不一致!';
	        -- 更新批次表状态和订单号
					update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT, ORDER_CODE='0' where ORDER_CODE = strOrderCode;
					commit;
	        return;
        end if;

        -- 根据订单号取得卡商、卡类型
        select FACTORY_NAME,FACTORY_CODE, CARD_TYPE into temp_factoryName,temp_factoryCode, temp_cardType from DM_BATCH where ORDER_CODE = strOrderCode and rownum=1;

        -- 计算订单总数量
        select sum(BATCH_COUNT) into temp_count from DM_BATCH where ORDER_CODE = strOrderCode;

        -- 在订单信息表中插入相应的纪录
        insert into DM_ORDER values(strOrderCode,temp_factoryName,temp_cardType,temp_count,sysdate,DM_PKG_CONST.BACTH_STATE_GENERATE,strOrderType,temp_factoryCode);
		commit;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
				update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT, ORDER_CODE='0' where ORDER_CODE = strOrderCode;
				commit;
	end OrderInfoAdd;

	-----------------------------------------------------------------------------------------
	-- 名称: OrderInfoDel
	-- 功能: 删除订单信息
	-- 调用: { call DM_PKG_DATA_MNG.OrderInfoDel(?,?,?) }
	-- 参数:
	--	strOrderCode	in	varchar2		订单号
	--	retid			out	number			成功/失败信息ID
	--	retmsg			out	varchar2		成功/失败信息描述
	-- 返回值:
	--	retid			:= 5012;
	--	retmsg			:= '输入的卡商订单号在订单信息表中不存在！';
	--	retid			:= 5013;
	--	retmsg			:= '输入的卡商订单号在批次信息表中不存在！';
	--	retid			:= 5014;
	--	retmsg			:= '将要删除的订单已经提交给卡商！';
	--	retid			:= 0;
	--	retmsg			:= '成功!';
	-----------------------------------------------------------------------------------------
	procedure OrderInfoDel(strOrderCode in varchar2, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
		temp_batch_num varchar2(9):= null;
    temp_order_lbyk varchar2(20):= null;
	begin
		-- 判断输入的订单号在订单信息表中是否存在
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE = strOrderCode;
		if temp_flag < 1 then
			retid	:= 5012;
			retmsg	:= '输入的卡商订单号在订单信息表中不存在！';
			return;
		end if;

		-- 判断输入的订单号在批次信息表中是否存在
		select count(*) into temp_flag from DM_BATCH where ORDER_CODE = strOrderCode;
		if temp_flag < 1 then
			retid	:= 5013;
			retmsg	:= '输入的卡商订单号在批次信息表中不存在！';
			return;
		end if;

		
	 	select count(*) into temp_flag from DM_ORDER where ORDER_CODE = strOrderCode and NOTES = DM_PKG_CONST.ORDER_TYPE_LBYK;
   	if temp_flag > 0 then
      temp_order_lbyk := DM_PKG_CONST.BK_CARD_TYPE_LBYK || strOrderCode;
	   	select count(*) into temp_flag from DM_BLANK_CARD_BATCH where NOTES = temp_order_lbyk;
	  	if temp_flag > 0 then
	   		select BATCH_NUM into temp_batch_num from DM_BLANK_CARD_BATCH where NOTES = temp_order_lbyk;
		  	DM_PKG_BLANK_CARD_MNG.BlankCardBacthDel(temp_batch_num, retid, retmsg);
		  	if retid <> 0 then
		  		return;
  			end if;
	   	end if;
   	end if;

		delete from DM_ORDER where ORDER_CODE = strOrderCode;
		update DM_BATCH set ORDER_CODE = '0',BATCH_STATUS = DM_PKG_CONST.BACTH_STATE_INIT where ORDER_CODE = strOrderCode;
  	commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end OrderInfoDel;

	-----------------------------------------------------------------------------------------
	-- 名称: OrderInfoList
	-- 功能: 按条件查询订单列表
	-- 调用: { call DM_PKG_DATA_MNG.OrderInfoList(?,?,?,?,?,?,?,?,?,?,?) }
	-- 参数:
	--	strOrderType	in	varchar2			订单类型：1-非两不一快，2-两不一快，3-IMSI回收实体卡，4-IMSI回收两不一快
	--	strOrderNum		in	varchar2			订单号
	--	strFactoryName	in	varchar2			卡商名称
	--	strStartTime	in	varchar2			起始时间
	--	strEndTime		in	varchar2			结束时间
	--	iPage			in	number				当前页码
	--	iPageSize		in	number				每页显示的记录条数
	--	records			out	number				查询出的记录条数
	--	results			out	cursorType			游标类型结果集
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	records			:= 查询出的记录条数
	--	results			:= 游标类型结果集
	--	retid			:= 0;
	--	retmsg			:= '成功!';
	-----------------------------------------------------------------------------------------
	procedure OrderInfoList(strOrderType in varchar2, strOrderNum in varchar2, strFactoryCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
		temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
		temp_minPage number(11) := -1;
		temp_maxPage number(11) := -1;
	begin
		temp_minPage := ((iPage - 1) * iPageSize) + 1;
		temp_maxPage := iPage * iPageSize;

		if strOrderNum is null then
			if strFactoryCode is null then
				if strStartTime is null and strEndTime is null then
				-- 所有过滤条件为空时
					select count(*) into records from DM_ORDER where NOTES=strOrderType;
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				else
				-- 过滤条件：按【时间段】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				end if;
			else
				if strStartTime is null and strEndTime is null then
				-- 过滤条件：按【卡商编码】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and FACTORY_CODE=strFactoryCode;
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and FACTORY_CODE=strFactoryCode order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				else
				-- 过滤条件：按【时间段+卡商名称】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				end if;
			end if;
		else
			if strFactoryCode is null then
				if strStartTime is null and strEndTime is null then
				-- 过滤条件：按【订单号】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and ORDER_CODE=strOrderNum;
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and ORDER_CODE=strOrderNum order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				else
				-- 过滤条件：按【订单号+时间段】 查询
					select count(*) into records from DM_ORDER where ORDER_CODE=strOrderNum and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and ORDER_CODE=strOrderNum and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				end if;
			else
				if strStartTime is null and strEndTime is null then
				-- 过滤条件：按【订单号+卡商编码】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_CODE=strOrderNum;
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_CODE=strOrderNum order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				else
				-- 过滤条件：按【订单号+时间段+卡商编码】查询
					select count(*) into records from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_CODE=strOrderNum and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
					open results for
						select * from (
							select ORDER_CODE,FACTORY_NAME,CARD_TYPE,ORDER_COUNT,to_char(ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME,ORDER_STATUS,NOTES, ROWNUM NUM
							from (select * from DM_ORDER where NOTES=strOrderType and  FACTORY_CODE=strFactoryCode and ORDER_CODE=strOrderNum and ORDER_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by ORDER_CODE desc)
						) where NUM between temp_minPage and temp_maxPage;
				end if;
			end if;
		end if;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end OrderInfoList;

	-----------------------------------------------------------------------------------------
	-- 名称: OrderInfoGenList
	-- 功能: 需要生成订单的列表
	-- 调用: { call DM_PKG_DATA_MNG.OrderInfoGenList(?,?,?,?,?,?,?,?) }
	-- 参数:
  --	strFactoryCode	in	varchar2			卡商编码 add at 2020-05-11
	--	strFactoryName	in	varchar2			卡商名称
	--	strCardName		in	varchar2			卡名称
	--	strStartTime	in	varchar2			起始时间
	--	strEndTime		in	varchar2			结束时间
	--	records			out	number				查询出的记录条数
	--	results			out	cursorType			游标类型结果集
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	records			:= 查询出的记录条数
	--	results			:= 游标类型结果集
	--	retid			:= 5015;
	--	retmsg			:= '卡商名[DM_PKG_CONST.FACTORY_NAME_RPS]不能生成订单。';
	--	retid			:= 0;
	--	retmsg			:= '成功!';
	-----------------------------------------------------------------------------------------
	procedure OrderInfoGenList(strFactoryCode in varchar2,strFactoryName in varchar2, strCardName in varchar2, strStartTime in varchar2, strEndTime in varchar2, records out number, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
		temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
	begin
		if strFactoryName=DM_PKG_CONST.FACTORY_NAME_RPS then
			retid	:= 5015;
			retmsg	:= '卡商名[' || DM_PKG_CONST.FACTORY_NAME_RPS || ']不能生成订单。';
			return;
		end if;

		if strStartTime is null and strEndTime is null then
		-- 时间段为空时
			select count(*) into records from DM_BATCH where FACTORY_CODE=strFactoryCode and CARD_TYPE=strCardName and BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT and PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT;
			open results for
				select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
					   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
				from DM_BATCH where  FACTORY_CODE=strFactoryCode and CARD_TYPE=strCardName and BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT and PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT order by BATCH_NUM desc;
		else
		-- 过滤条件：按【时间段】查询
			select count(*) into records from DM_BATCH
				where BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
				and  FACTORY_CODE=strFactoryCode and CARD_TYPE=strCardName and BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT and PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT;
			open results for
				select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
					   ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM
				from DM_BATCH
				where BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
				and  FACTORY_CODE=strFactoryCode and CARD_TYPE=strCardName and BATCH_STATUS=DM_PKG_CONST.DATA_STATE_INIT and PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT
				order by BATCH_NUM desc;
		end if;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end OrderInfoGenList;

	
	procedure OrderDetailInfo(strOrderNum in varchar2, results out cursorType, detailInfo out cursorType, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
	begin
		select COUNT(*) into temp_flag from DM_ORDER where ORDER_CODE=strOrderNum;
		if temp_flag < 1 then
			retid	:= 5016;
			retmsg	:= '指定的订单[' || strOrderNum || ']信息在订单表中不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_BATCH where ORDER_CODE=strOrderNum;
		if temp_flag < 1 then
			retid	:= 5017;
			retmsg	:= '指定的订单[' || strOrderNum || ']信息在批次表中不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_ORDER a, DM_FACTORY b, DM_COMPANY c where a.ORDER_CODE=strOrderNum and a.FACTORY_NAME=b.FACTORY_NAME;
		if temp_flag < 1 then
			retid	:= 5018;
			retmsg	:= '指定的订单[' || strOrderNum || ']信息在卡商表或移动省公司表中不存在。';
			return;
		end if;

		open results for
			select a.ORDER_CODE, a.FACTORY_NAME,a.FACTORY_CODE, a.CARD_TYPE, a.ORDER_COUNT, to_char(a.ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME, a.ORDER_STATUS,a.NOTES,
				   b.FACTORY_ID,b.FACTORY_CODE,b.CONTACT_PERSON as FAC_CON_PERSON,b.CONTACT_TEL as FAC_CON_TEL,b.FAX_NUM as FAC_CON_FAX,b.POST_NUM,b.POST_ADDR,
				   c.COMPANY_NAME, c.CONTACT_PERSON as COM_CON_PERSON, c.CONTACT_TEL as COM_CON_TEL, c.FAX_NUM as COM_CON_FAX, c.E_MAIL
			from DM_ORDER a, DM_FACTORY b, DM_COMPANY c
			where a.ORDER_CODE=strOrderNum and a.FACTORY_NAME=b.FACTORY_NAME;

		open detailInfo for
			select BATCH_NUM,BATCH_REGION,FACTORY_CODE,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
           ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES
      from DM_BATCH where ORDER_CODE=strOrderNum order by BATCH_NUM;

    retid  := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end OrderDetailInfo;


	
	procedure OrderStateModify(strOrderNum in varchar2, iStatus in number, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
	begin
		-- 判断输入的订单号是否存在！
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE = strOrderNum;
		if temp_flag < 1 then
			retid	:= 6013;
			retmsg	:= '输入的订单号不存在！';
			return;
		end if;

		update DM_ORDER set ORDER_STATUS=iStatus where ORDER_CODE = strOrderNum;
		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg := sqlerrm;
				rollback;
	end OrderStateModify;


  procedure StatusCount(iStatus in number, records out number, retid out number, retmsg out varchar2)
  is
  begin
    -- 按状态统计订单信息
    select count(*) into records from DM_ORDER where ORDER_STATUS=iStatus;

    retid  := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end StatusCount;


 
	procedure OrderImport(strRegionName in varchar2, strCardName in varchar2, strCount in number, strFactoryName in varchar2, strFactoryCode in varchar2, strDataSeg in varchar2, retid out number, retmsg out varchar2)
	is
		temp_flag number := -1;
		temp_attr_id varchar2(5) := null;
		temp_smsp varchar2(14) := null;
		temp_region_code varchar2(20) := null;
	    temp_region_name varchar2(20) := null;
	    temp_hlr_code varchar2(20) := null;
	    temp_hlr_name varchar2(20) := null;
		temp_count number := -1;
		temp_count_assign number := -1;
		temp_imsi_count number := -1;
		temp_imsi_seg varchar2(11) := '';
		temp_imsi_type varchar2(2) := null;
		temp_batchNum varchar2(20) := null;
		temp_batchNums varchar2(1024) := null;
		temp_orderCode varchar2(20) := null;

		temp_cursor_dataSeg   cursorType := null;
	    temp_cursor_imsi    cursorType := null;
	    temp_cursor_batch   cursorType := null;
	    temp_sql            varchar2(1024) := '';
	    temp_sql_dataSeg    varchar2(1024) := null;
	    temp_sql_imsi    varchar2(1024) := null;
	    temp_sql_batch    varchar2(4096) := null;

	    temp_data_seg varchar2(7) := null;
	    temp_iccid_start varchar2(20) := null;
	    temp_iccid_end varchar2(20) := null;
	    temp_count_start number := -1;
	    temp_msisdn_count number := -1;
	    temp_imsi_start varchar2(15) := null;
	    temp_imsi_end varchar2(15) := null;
	    temp_msisdn_start varchar2(11) := null;
	    temp_msisdn_end varchar2(15) := null;

		temp_next varchar2(32767) := ' ';
	    temp_retid number := -1;
    	temp_retmsg varchar2(1024) := null;
	begin
		select COUNT(*) into temp_flag from DM_REGION where REGION_NAME=strRegionName;
		if temp_flag < 1 then
			retid	:= 6001;
			retmsg	:= '地市名为[' || strRegionName || ']的地市不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_CARD_TYPE a where a.CARD_NAME=strCardName;
		if temp_flag < 1 then
			retid	:= 6002;
			retmsg	:= '卡类型[' || strCardName || ']不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_FACTORY a where a.FACTORY_CODE=strFactoryCode;
		if temp_flag < 1 then
			retid	:= 6003;
			retmsg	:= '卡商代码为[' || strFactoryCode || ']的卡商不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from DM_DATA_SEG a where substr(a.DATA_SEG, 0, length(strDataSeg)) = strDataSeg;
		if temp_flag < 1 then
			retid	:= 6004;
			retmsg	:= '号段[' || strDataSeg || ']不存在。';
			return;
		end if;

		select COUNT(*) into temp_flag from
			(select l.*, l2.attr_name as parent_name from dm_hlr_link l join dm_hlr_link l2 on l.parent_id = l2.attr_id
			       where l.attr_level > 1
			)l, dm_region r, dm_hlr h where l.attr_name = h.hlr_name and l.parent_name = r.region_name and h.region_code = r.region_code
			and r.region_name = strRegionName and attr_id in (select attr_id from dm_data_seg where substr(DATA_SEG, 0, length(strDataSeg)) = strDataSeg);
		if temp_flag < 1 then
			retid	:= 6005;
			retmsg	:= '号段与地市名不匹配。';
			return;
		end if;

		-- 计算号段下可用IMSI数据
		select sum(imsi_count) into temp_count from DM_IMSI where ( status = DM_PKG_CONST.IMSI_STATE_UNUSED or status = DM_PKG_CONST.IMSI_STATE_USED ) and data_seg in
			(select d.data_seg from
			(select l.*, l2.attr_name as parent_name from dm_hlr_link l join dm_hlr_link l2 on l.parent_id = l2.attr_id
			       where l.attr_level > 1
			)l, dm_region r, dm_hlr h, dm_data_seg d where l.attr_name = h.hlr_name and l.parent_name = r.region_name and h.region_code = r.region_code and l.attr_id = d.attr_id
			and r.region_name = strRegionName and substr(DATA_SEG, 0, length(strDataSeg)) = strDataSeg);
		if temp_count < strCount then
			retid	:= 6006;
			retmsg	:= '该号段下可用IMSI数据数据量不足。';
			return;
		end if;

		select distinct r.region_code, h.hlr_name, h.hlr_code, l.attr_id into temp_region_code, temp_hlr_name, temp_hlr_code, temp_attr_id from
			(select l.*, l2.attr_name as parent_name from dm_hlr_link l join dm_hlr_link l2 on l.parent_id = l2.attr_id
			       where l.attr_level > 1
			)l, dm_region r, dm_hlr h, dm_data_seg d where l.attr_name = h.hlr_name and l.parent_name = r.region_name and h.region_code = r.region_code and l.attr_id = d.attr_id
			and r.region_name = strRegionName and substr(DATA_SEG, 0, length(strDataSeg)) = strDataSeg;

		temp_count := strCount;
		if length(strDataSeg) = 7 then
			temp_data_seg := strDataSeg;

			temp_sql_imsi := 'select imsi_seg, imsi_count, imsi_type from DM_IMSI where (status = ' || DM_PKG_CONST.IMSI_STATE_UNUSED || ' or status = ' || DM_PKG_CONST.IMSI_STATE_USED || ' ) and data_seg = ''' || temp_data_seg || ''' order by imsi_count desc, imsi_seg desc';

			--从IMSI表中循环取出IMSI段，再根据取出的imsi生成批次
		    open temp_cursor_imsi for temp_sql_imsi;
		    if  temp_cursor_imsi %isopen then
		        loop
		          	fetch temp_cursor_imsi into temp_imsi_seg,temp_imsi_count,temp_imsi_type;
		          	exit when temp_count = 0 or temp_cursor_imsi %notfound;

		          	if temp_count > temp_imsi_count then
		            	temp_count_assign := temp_imsi_count;
		            	temp_count := temp_count - temp_imsi_count;
		            else
		                temp_count_assign := temp_count;
		                temp_count := 0;
		            end if;

		            -- 生成批次号和短信中心
		            DM_PKG_DATA_ASSIGN.GenerateBatchInfoByAttrId(temp_attr_id,temp_batchNum,temp_smsp,temp_region_name,retid,retmsg);
		            if retid <> 0 then
	                	-- 获取批次号失败,删除已生成的批次，直接返回错误信息
	                	while (temp_next is not null) loop
							DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
							temp_batchNums := temp_next;

							BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
						end loop;

	               		return;
	                end if;

	                -- 计算数量生成IMSI起始终止、MSISDN起始终止、ICCID起始终止
	                temp_count_start := DM_PKG_CONST.IMSI_UNUSED_COUNT - temp_imsi_count;
                	temp_imsi_start := temp_imsi_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');
                	temp_msisdn_start := temp_data_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');

                	temp_count_start := DM_PKG_CONST.IMSI_UNUSED_COUNT - temp_imsi_count + temp_count_assign - 1;
                	temp_imsi_end := temp_imsi_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');
                	temp_msisdn_end := temp_data_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');

                	DM_PKG_DATA_ASSIGN.GenerateICCID(temp_data_seg, temp_imsi_start, strFactoryCode, temp_count_assign, temp_iccid_start, temp_iccid_end, retid, retmsg);
                	if retid <> 0 then
	                	-- 获取ICCID起始和终止失败,删除已生成的批次，直接返回错误信息
	                	while (temp_next is not null) loop
							DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
							temp_batchNums := temp_next;

							BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
						end loop;

	                	return;
	                end if;

	                -- 分配数据生成批次
	                DM_PKG_DATA_ASSIGN.DataAssign(temp_attr_id, temp_batchNum, strFactoryCode, strFactoryName, strCardName, temp_imsi_type, temp_msisdn_start, temp_msisdn_end, temp_imsi_start, temp_imsi_end, temp_iccid_start, temp_iccid_end, temp_count_assign, '订单导入批次生成', retid, retmsg);
	                if retid <> 0 then
	                	-- 数据分配失败,删除已生成的批次，直接返回错误信息
	                	while (temp_next is not null) loop
							DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
							temp_batchNums := temp_next;

							BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
						end loop;

	                	return;
	                end if;

	                if temp_batchNums is null then
			            temp_batchNums := temp_batchNum;
			        else
			            temp_batchNums := temp_batchNums || ',' || temp_batchNum;
			        end if;

		        end loop;
      		end if;
		else
			temp_sql_dataSeg := 'select d.data_seg from ';
			temp_sql_dataSeg := temp_sql_dataSeg || ' (select l.*, l2.attr_name as parent_name from dm_hlr_link l join dm_hlr_link l2 on l.parent_id = l2.attr_id where l.attr_level > 1)l, dm_region r, dm_hlr h, dm_data_seg d ';
			temp_sql_dataSeg := temp_sql_dataSeg || ' where l.attr_name = h.hlr_name and l.parent_name = r.region_name and h.region_code = r.region_code and l.attr_id = d.attr_id ';
			temp_sql_dataSeg := temp_sql_dataSeg || ' and r.region_name = ''' || strRegionName || ''' and substr(DATA_SEG, 0, length(''' || strDataSeg || ''')) = ''' || strDataSeg || ''' order by msisdn_count desc, data_seg ';

			--从号段表中循环取出号段，再根据号段取出imsi生成批次
		    open temp_cursor_dataSeg for temp_sql_dataSeg;
		    if temp_cursor_dataSeg is not null and temp_cursor_dataSeg %isopen then
		    	loop
		    		fetch temp_cursor_dataSeg into temp_data_seg;
		    		exit when temp_count = 0 or temp_cursor_dataSeg %notfound;

		    		temp_sql_imsi := 'select imsi_seg, imsi_count, imsi_type from DM_IMSI where (status = ' || DM_PKG_CONST.IMSI_STATE_UNUSED || ' or status = ' || DM_PKG_CONST.IMSI_STATE_USED || ' ) and data_seg = ''' || temp_data_seg || ''' order by imsi_count desc, imsi_seg desc';

					--从IMSI表中循环取出IMSI段，再根据取出的imsi生成批次
				    open temp_cursor_imsi for temp_sql_imsi;
				    if temp_cursor_imsi is not null and temp_cursor_imsi %isopen then
				        loop
				          	fetch temp_cursor_imsi into temp_imsi_seg,temp_imsi_count,temp_imsi_type;
				          	exit when temp_count = 0 or temp_cursor_imsi %notfound;

				          	if temp_count > temp_imsi_count then
				            	temp_count_assign := temp_imsi_count;
				            	temp_count := temp_count - temp_imsi_count;
				            else
				                temp_count_assign := temp_count;
				                temp_count := 0;
				            end if;

				            -- 生成批次号和短信中心
				            DM_PKG_DATA_ASSIGN.GenerateBatchInfoByAttrId(temp_attr_id,temp_batchNum,temp_smsp,temp_region_name,retid,retmsg);
				            if retid <> 0 then
			                	-- 获取批次号失败,删除已生成的批次，直接返回错误信息
			                	while (temp_next is not null) loop
									DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
									temp_batchNums := temp_next;

									BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
								end loop;

			               		return;
			                end if;

			                -- 计算数量生成IMSI起始终止、MSISDN起始终止、ICCID起始终止
			                temp_count_start := DM_PKG_CONST.IMSI_UNUSED_COUNT - temp_imsi_count;
		                	temp_imsi_start := temp_imsi_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');
		                	temp_msisdn_start := temp_data_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');

		                	temp_count_start := DM_PKG_CONST.IMSI_UNUSED_COUNT - temp_imsi_count + temp_count_assign - 1;
		                	temp_imsi_end := temp_imsi_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');
		                	temp_msisdn_end := temp_data_seg || DM_PKG_UTILS.padding(to_char(temp_count_start),'0',4,'L');

		                	DM_PKG_DATA_ASSIGN.GenerateICCID(temp_data_seg, temp_imsi_start, strFactoryCode, temp_count_assign, temp_iccid_start, temp_iccid_end, retid, retmsg);
		                	if retid <> 0 then
			                	-- 获取ICCID起始和终止失败,删除已生成的批次，直接返回错误信息
			                	while (temp_next is not null) loop
									DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
									temp_batchNums := temp_next;

									BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
								end loop;

			                	return;
			                end if;

			                -- 分配数据生成批次
			                DM_PKG_DATA_ASSIGN.DataAssign(temp_attr_id, temp_batchNum, strFactoryCode, strFactoryName, strCardName, temp_imsi_type, temp_msisdn_start, temp_msisdn_end, temp_imsi_start, temp_imsi_end, temp_iccid_start, temp_iccid_end, temp_count_assign, '订单导入批次生成', retid, retmsg);
			                if retid <> 0 then
			                	-- 数据分配失败,删除已生成的批次，直接返回错误信息
			                	while (temp_next is not null) loop
									DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_batchNum, temp_next);
									temp_batchNums := temp_next;

									BatchInfoDelete(temp_batchNum, temp_retid, temp_retmsg);
								end loop;

			                	return;
			                end if;

			                if temp_batchNums is null then
			                	temp_batchNums := temp_batchNum;
			                else
			                	temp_batchNums := temp_batchNums || ',' || temp_batchNum;
			                end if;

				        end loop;
		      		end if;

		    	end loop;
		    end if;

		end if;

		
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end OrderImport;


  procedure updateBathDataType(strBatchNum in varchar2,strBatchDataType in varchar2, retid out number, retmsg out varchar2)
  is
	temp_flag number := -1;
  begin
		-- 判断输入的批次号是否存在！
		select count(*) into temp_flag from dm_batch where batch_num = strBatchNum;
		if temp_flag < 1 then
			retid	:= 6014;
			retmsg	:= '输入的批次号不存在！';
			return;
		end if;

		update dm_batch set NOTES=strBatchDataType where batch_num = strBatchNum;
		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg := sqlerrm;
				rollback;
  end updateBathDataType;

  
  procedure GetOrderStatus(strOrderCode in varchar2,strOrderStatus out varchar2, retid out number, retmsg out varchar2)
  is
	temp_flag number := -1;
  begin
		-- 判断输入的订单号是否存在！
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE = strOrderCode;
		if temp_flag < 1 then
		  retid  := 6013;
		  retmsg  := '输入的订单号不存在！';
		  return;
		end if;

		select ORDER_STATUS into strOrderStatus from DM_ORDER where  ORDER_CODE = strOrderCode;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg := sqlerrm;
				rollback;
  end GetOrderStatus;

end DM_PKG_DATA_MNG;
//
delimiter ;//