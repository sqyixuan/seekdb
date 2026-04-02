delimiter //;

create or replace package pdms.DM_PKG_IMSI_CYC is
	
	type cursorType is ref cursor;

	procedure IMSICycImport(strSeqNum in varchar2, strRegionCode in varchar2, iCount in number, retid out number, retmsg out varchar2);

	procedure IMSICycXhImport(strSeqNum in varchar2, strRegionCode in varchar2, iCount in number, retid out number, retmsg out varchar2);

	procedure IMSIAutoCycImport(retid out number, retmsg out varchar2);

	procedure IMSICycStatistics(strAttrId in number, strDataSeg in varchar2, iPage in number, iPageSize in number, results out cursorType, retid out number, retmsg out varchar2, records out number);

	procedure GenerateIMSICycICCID(strAttrId in number, strDataSeg in varchar2, strFactoryCode in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2,retid out number, retmsg out varchar2);

	procedure IMSICycDataAssign(strAttrId in number, strBatchNum in varchar2,strFactoryCode in varchar2,strFactoryName in varchar2,strCardName in varchar2, strDataSeg in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2);

  	procedure IMSICycXhDataAssign(strRegionShort in varchar2, strXHDataSeg in varchar2, strBatchNum in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure IMSICycBatchDel(strBatchNum in varchar2, retid out number, retmsg out varchar2);

  	procedure XHCycBatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2);

	procedure IMSICycDataGenForRPS(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure XHIMSICycDataGenForRPS(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure IMSICycDataRollback(strBatchNum in varchar2, retid out number, retmsg out varchar2);

	procedure XHIMSICycDataRollback(strBatchNum in varchar2, retid out number, retmsg out varchar2);

	procedure IMSICycDataGenForOrder(strOrderNum in varchar2, strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure IMSICycDataGenForLBYK(strOrderNum in varchar2, retid out number, retmsg out varchar2);

	procedure GetBatchListByOrder(strOrderNum in varchar2, results out cursorType, detailInfo out cursorType, retid out number, retmsg out varchar2);

	procedure GenerateEKiList(iNum in number, results out cursorType, retid out number, retmsg out varchar2);

	procedure IMSICycBatchList(strDataSeg in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

	procedure XHIMSICycBatchList(strRegionCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

	procedure IMSICycBackupDataDel;

	procedure IMSICycBacthInfo(strSeqNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	PROCEDURE IMSIExportCycErrInfos(strSeqNum IN VARCHAR2,results OUT cursorType, retid OUT NUMBER, retmsg OUT varchar2);

end DM_PKG_IMSI_CYC;
//

create or replace package body pdms.DM_PKG_IMSI_CYC is
	
	procedure IMSICycImport(strSeqNum in varchar2, strRegionCode in varchar2, iCount in number, retid out number, retmsg out varchar2)
	is
		temp_flag			number := 0;
		temp_region_short	VARCHAR2(20) :='';
		temp_region_name	VARCHAR2(20) :='';
		temp_hlr_code		VARCHAR2(32) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_notes			VARCHAR2(100) :='';
		temp_cursor 		cursorType := null;
		temp_attr_id		VARCHAR2(3) := '';
		temp_attr_name		VARCHAR2(24) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_str			VARCHAR2(1024) :='';
	begin
		dbms_output.put_line(1);
		select count(*) into temp_flag from DM_IMSI_CYC_TEMP where seq_num=strSeqNum;
		if temp_flag<>iCount then
			retid	:= 8001;
			retmsg	:= '该批数据数量不正确，请确认!';
			return;
		end if;
		-- 检查IMSI是否有重复
		select count(distinct IMSI) into temp_flag from DM_IMSI_CYC_TEMP where seq_num=strSeqNum;
		if temp_flag<>iCount then
			retid	:= 8002;
			retmsg	:= '该批数据有重复的IMSI，请确认!';
			return;
		end if;

		-- 检查同一批IMSI是否属于同一地市
		select count(distinct REGION_CODE) into temp_flag from DM_IMSI_CYC_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8003;
			retmsg	:= '序列为['||strSeqNum||']的一批数据不在同一地市下，请确认!';
			return;
		end if;

		-- 检查同一批IMSI是否属于同一号段
		select count(distinct DATA_SEG) into temp_flag from DM_IMSI_CYC_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8096;
			retmsg	:= '序列为['||strSeqNum||']的一批数据不属于同一号段，请确认!';
			return;
		end if;

		-- 检查同一批IMSI前10位是否相同
		select count(distinct substr(IMSI,1,10)) into temp_flag from DM_IMSI_CYC_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8097;
			retmsg	:= '序列为['||strSeqNum||']的一批数据IMSI前10位不同，请确认!';
			return;
		end if;

		-- 根据地市代码查询地市缩写
		select count(*) into temp_flag from DM_REGION where REGION_CODE=strRegionCode;
		if temp_flag<1 then
			retid	:= 8004;
			retmsg	:= '地市代码为['|| strRegionCode || ']信息不存在，请确认!';
			return;
		end if;
		select REGION_SHORT,REGION_NAME into temp_region_short,temp_region_name from DM_REGION where REGION_CODE=strRegionCode;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 查询IMSI数据
		open temp_cursor for select HLR_CODE,DATA_SEG,IMSI,NOTES from DM_IMSI_CYC_TEMP where SEQ_NUM=strSeqNum;
		-- 遍历IMSI数据
		if  temp_cursor %isopen then
			loop
				fetch temp_cursor into temp_hlr_code,temp_data_seg,temp_imsi,temp_notes;
				exit when temp_cursor %notfound;

				-- 检查与地市表据是否有重复的数据  -优化： 2014-8-19  验证IMSI重复移到遍历里
				temp_str := 'select count(*) from '|| temp_table_name || ' where IMSI=:1';
				execute immediate temp_str into temp_flag using temp_imsi;
				if temp_flag>0 then
					retid	:= 8005;
					retmsg	:= '该批数据在IMSI回收地市表中有重复的IMSI[' || temp_imsi || '，请确认!';
					return;
				end if;

				-- 检查号段是否存在
				select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=temp_data_seg;
				if temp_flag<1 then
					retid	:= 8006;
					retmsg	:= 'IMSI['||temp_imsi||']的号段在号段表中不存在!';
					return;
				end if;
				-- 查询号段对应的ATTR_ID
				select ATTR_ID into temp_attr_id from DM_DATA_SEG where DATA_SEG=temp_data_seg;

				-- 检查号段与网段是否对应
				if temp_hlr_code<>substr(temp_data_seg,1,3) then
					retid	:= 8007;
					retmsg	:= 'IMSI['||temp_imsi||']的号段与网段不对应!';
					return;
				end if;

				-- 检查号段与IMSI是否对应
				select count(*) into temp_flag from DM_IMSI where DATA_SEG=temp_data_seg and IMSI_SEG=substr(temp_imsi,1,11);
				if temp_flag<1 then
					retid	:= 8008;
					retmsg	:= '号段['||temp_data_seg||']对应的IMSI段['||substr(temp_imsi,1,11)||']在IMSI表中不存在!';
					return;
				end if;

				-- 检查号段与地市是否对应
				select c.ATTR_NAME into temp_attr_name from DM_DATA_SEG a, DM_HLR_LINK b, DM_HLR_LINK c
				where a.DATA_SEG=temp_data_seg and a.ATTR_ID=b.ATTR_ID and b.PARENT_ID=c.ATTR_ID;
				if temp_region_name<>temp_attr_name then
					retid	:= 8009;
					retmsg	:= 'IMSI['||temp_imsi||']对应的地市不正确!';
					return;
				end if;

			end loop;
		end if;
		--关闭游标
		close temp_cursor;

		-- IMSI回收正式表中插入数据  -优化： 2014-8-19  复制临时表导入
		temp_str := 'insert into ' || temp_table_name || ' (SEQ_NUM,REGION_CODE,HLR_CODE,ATTR_ID,DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES) ';
		temp_str := temp_str || 'select SEQ_NUM,REGION_CODE,HLR_CODE,' || temp_attr_id || ',DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES from DM_IMSI_CYC_TEMP where SEQ_NUM=:1';
		execute immediate temp_str using strSeqNum;

		-- 在IMSI回收批次表中插入一条记录， 四个空字符：1：notes 1：导入状态 2：err 3：succ 4：importType
		insert into DM_IMSI_CYC_BATCH values(strSeqNum,strRegionCode,temp_hlr_code,temp_attr_id,temp_data_seg,iCount,sysdate,'',DM_PKG_CONST.BATCH_CYC_IMSI_STATE_1,'0',iCount,DM_PKG_CONST.IMSI_CYC_IMPORT_TYPE_0);

		-- 删除IMSI临时表中已转入正式表中的数据
		delete from DM_IMSI_CYC_TEMP where SEQ_NUM=strSeqNum;
		commit;

		retid	:= 0;
		retmsg	:= '回收IMSI导入成功。';

		-- 系统异常处理
		exception
			when others then
				rollback;
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end IMSICycImport;

	
	procedure IMSICycXhImport(strSeqNum in varchar2, strRegionCode in varchar2, iCount in number, retid out number, retmsg out varchar2)
	is
		temp_flag			number := 0;
		temp_region_short	VARCHAR2(2) :='';
		temp_region_name	VARCHAR2(20) :='';
		temp_hlr_code		VARCHAR2(32) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_notes			VARCHAR2(100) :='';
		temp_cursor 		cursorType := null;
		temp_attr_id		VARCHAR2(3) := '';
		temp_attr_name		VARCHAR2(24) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_str			VARCHAR2(1024) :='';
	begin
		-- 检查IMSI导入数量
		select count(*) into temp_flag from DM_IMSI_CYC_XH_TEMP where seq_num=strSeqNum;
		if temp_flag<>iCount then
			retid	:= 8001;
			retmsg	:= '该批数据数量不正确，请确认!';
			return;
		end if;
		-- 检查IMSI是否有重复
		select count(distinct IMSI) into temp_flag from DM_IMSI_CYC_XH_TEMP where seq_num=strSeqNum;
		if temp_flag<>iCount then
			retid	:= 8002;
			retmsg	:= '该批数据有重复的IMSI，请确认!';
			return;
		end if;

		-- 检查同一批IMSI是否属于同一地市
		select count(distinct REGION_CODE) into temp_flag from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8003;
			retmsg	:= '序列为['||strSeqNum||']的一批数据不在同一地市下，请确认!';
			return;
		end if;

		-- 检查同一批IMSI是否属于同一号段
		select count(distinct DATA_SEG) into temp_flag from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8096;
			retmsg	:= '序列为['||strSeqNum||']的一批数据不属于同一号段，请确认!';
			return;
		end if;

		-- 检查同一批IMSI前10位是否相同
		select count(distinct substr(IMSI,1,10)) into temp_flag from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=strSeqNum;
		if temp_flag>1 then
			retid	:= 8097;
			retmsg	:= '序列为['||strSeqNum||']的一批数据IMSI前10位不同，请确认!';
			return;
		end if;

		temp_table_name := 'DM_IMSI_CYC_XH';

		-- 查询IMSI数据
		open temp_cursor for select HLR_CODE,DATA_SEG,IMSI,NOTES from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=strSeqNum;
		-- 遍历IMSI数据
		if  temp_cursor %isopen then
			loop
				fetch temp_cursor into temp_hlr_code,temp_data_seg,temp_imsi,temp_notes;
				exit when temp_cursor %notfound;

				-- 检查与地市表据是否有重复的数据  -优化： 2014-8-19  验证IMSI重复移到遍历里
				temp_str := 'select count(*) from '|| temp_table_name || ' where IMSI=:1';
				execute immediate temp_str into temp_flag using temp_imsi;
				if temp_flag>0 then
					retid	:= 8005;
					retmsg	:= '该批数据在IMSI回收地市表中有重复的IMSI[' || temp_imsi || '，请确认!';
					return;
				end if;

			end loop;
		end if;
		--关闭游标
		close temp_cursor;

		-- IMSI回收正式表中插入数据  -优化： 2014-8-19  复制临时表导入
		temp_str := 'insert into ' || temp_table_name || ' (SEQ_NUM,REGION_CODE,HLR_CODE,ATTR_ID,DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES) ';
		temp_str := temp_str || 'select SEQ_NUM,REGION_CODE,HLR_CODE,ATTR_ID,DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=:1';
		execute immediate temp_str using strSeqNum;

		-- 在IMSI回收批次表中插入一条记录， 四个空字符：1：notes 1：导入状态 2：err 3：succ 4：importType
		insert into DM_IMSI_CYC_XH_BATCH values(strSeqNum,strRegionCode,'','',temp_data_seg,iCount,sysdate,'',DM_PKG_CONST.BATCH_CYC_IMSI_STATE_1,'0',iCount,DM_PKG_CONST.IMSI_CYC_IMPORT_TYPE_0);

		-- 删除IMSI临时表中已转入正式表中的数据
		delete from DM_IMSI_CYC_XH_TEMP where SEQ_NUM=strSeqNum;
		commit;

		retid	:= 0;
		retmsg	:= '回收IMSI导入成功。';

		-- 系统异常处理
		exception
			when others then
				rollback;
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end IMSICycXhImport;

	
	procedure IMSIAutoCycImport(retid out number, retmsg out varchar2)
	is
		temp_flag						NUMBER := 0;
		temp_region_cursor	cursorType := null;
		temp_region_code		VARCHAR2(5) :='';
		temp_region_short		VARCHAR2(20) :='';
		temp_region_name		VARCHAR2(20) :='';
		temp_cursor 				cursorType := null;
		temp_data_seg				VARCHAR2(7) :='';
		temp_count					NUMBER := 0;
		temp_errCount				NUMBER:=0;
		temp_succCount			NUMBER:=0;
		temp_errMeaasge			VARCHAR2(1024) :='';
		temp_dataSeg_status	NUMBER := -1;
		cyc_region_code			VARCHAR2(10);
		temp_hlr_code				VARCHAR2(32) :='';
		temp_imsi_min				VARCHAR2(15) :='';
		temp_imsi_max				VARCHAR2(15) :='';
		temp_imsiSeg				VARCHAR2(100) :='';
		temp_imsiSeg_err		VARCHAR2(200) :='';
		temp_imsiSeg11			VARCHAR2(11) :='';
		temp_imsi_status		NUMBER := -1;
		temp_imsi_cursor		cursorType := null;
		temp_imsi_count			NUMBER := 0;
		temp_imsi_index			NUMBER := 0;
		temp_attr_id				VARCHAR2(3) := '';
		temp_table_name			VARCHAR2(50) :='';
		temp_str						VARCHAR2(1024) :='';
		temp_err_str				VARCHAR2(1024) :='';
		temp_err_str_del		VARCHAR2(1024) :='';
		temp_err_str_copy		VARCHAR2(1024) :='';
		temp_err2_str				VARCHAR2(1024) :='';
		temp_err_str2				VARCHAR2(1024) :='';
		temp_seq_num				VARCHAR2(15) :='';
		temp_seq						VARCHAR2(3) :='';
		temp_row_num				NUMBER := 0;
		temp_i							NUMBER := -1;
		results VT;
		lenght varchar2(11);
	begin
		-- 判断是否存在回收数据
		select count(*) into temp_flag from DM_SYNC_IMSI_CYC t;
		if temp_flag < 1 then
			-- 在IMSI回收错误表中插入一条记录
			insert into DM_SYNC_IMSI_CYC_ERR_REOCRD values(concat(TO_CHAR(SYSDATE,'yymmddhh24miss'),'000'),'0000','000','0000000','000000000000000',3,sysdate,'当日无回收数据',sysdate);
			commit;
			return;
		end if;
		-- 查询地市列表
		OPEN temp_region_cursor FOR select t.region_code,t.region_short from dm_region t order by t.region_code;
		-- 按地市循环处理
		IF temp_region_cursor %isOPEN THEN
		LOOP
			FETCH temp_region_cursor INTO temp_region_code,temp_region_short;
			EXIT WHEN temp_region_cursor %notfound;
			-- 获取该地市数据量最多的前X个号段，X=DM_PKG_CONST.IMSI_AUTO_CYC_LIMIT_MAX
			OPEN temp_cursor FOR select * from (
				select a.data_seg,rownum from (
					select t.data_seg as data_seg,count(t.data_seg) from DM_SYNC_IMSI_CYC t
					where t.region_code=temp_region_code and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_0
					group by t.data_seg order by count(t.data_seg) desc) a
				) where rownum between 1 and DM_PKG_CONST.IMSI_AUTO_CYC_LIMIT_MAX;
			<<dataSegCycles>>
			IF temp_cursor %isOPEN THEN
			-- 按号段循环回收
			LOOP
				FETCH temp_cursor INTO temp_data_seg,temp_row_num;
				EXIT WHEN temp_cursor %notfound;
				temp_i := 0;
				-- 插入错误表语句
				temp_err_str := 'INSERT INTO DM_SYNC_IMSI_CYC_ERR_REOCRD (SEQ_NUM,REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,OPRATE_TIME) ';
				temp_err_str := temp_err_str || 'SELECT SEQ_NUM,REGION_CODE,HLR_CODE,DATA_SEG,IMSI,:1,GENERATE_TIME,:2,:3 FROM DM_IMSI_CYC_TEMP t WHERE seq_num=:4 ';

				-- 错误数据复制到BOSS共享错误表中
				temp_err2_str := 'INSERT INTO DM_SYNC_IMSI_CYC_ERROR (REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,OPRATE_TIME) ';
				temp_err2_str := temp_err2_str || ' SELECT REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,sysdate FROM DM_SYNC_IMSI_CYC_ERR_REOCRD WHERE seq_num=:1';

				-- 生成序列号
				temp_seq_num := TO_CHAR(SYSDATE,'yymmddhh24miss');
				temp_seq := floor(DBMS_RANDOM.VALUE(0,999));
				if temp_seq < 0 or temp_seq > 999 then
					retid	:= 8046;
					retmsg	:= '产生随机序列失败!';
					return;
				end if;
				temp_seq := DM_PKG_UTILS.padding(temp_seq, '0', 3, 'L');
				temp_seq_num := temp_seq_num || temp_seq;
				-- 回收表中更新数据状态
				update DM_SYNC_IMSI_CYC t set STATUS=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1 where t.region_code=temp_region_code
				and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_0;
				commit;
				-- 复制到临时表处理（注意:1.临时表和正式表中的状态与自动回收共享表状态要区分,2.临时表中的生成时间暂时用于拷贝导入时间）
				insert into DM_IMSI_CYC_TEMP (SEQ_NUM,REGION_CODE,HLR_CODE,DATA_SEG,IMSI,STATUS,GENERATE_TIME)
					(select temp_seq_num,REGION_CODE,NET_SEG,DATA_SEG,IMSI,DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED,CYC_TIME from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code
						and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1);
				commit;
				-- 以下开始操作临时表，查询数量
				select count(*) into temp_count from DM_IMSI_CYC_TEMP t where t.SEQ_NUM = temp_seq_num;
				-- 插入回收批次信息
				insert into DM_IMSI_CYC_BATCH values(temp_seq_num,temp_region_code,substr(temp_data_seg,1,3),0,temp_data_seg,temp_count,sysdate,'',DM_PKG_CONST.BATCH_CYC_IMSI_STATE_0,0,0,DM_PKG_CONST.IMSI_CYC_IMPORT_TYPE_1);
				commit;
				temp_errCount :=0;
				temp_succCount := temp_count;
				-- 检查号段是否存在
				select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=temp_data_seg;
				if temp_flag<1 then

					temp_errMeaasge := '该号段['||temp_data_seg||']在号段表中不存在!';
					--处理错误数据及对应数量
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;

					delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num;

					commit;
					temp_errCount := temp_count;
					temp_succCount := 0;
					EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;

					update DM_IMSI_CYC_BATCH set CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;

					-- 删除回收共享表中已处理的数据
					delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;

					commit;

					goto dataSegCycles;
				end if;
                dbms_output.put_line(11);
				-- 查询号段对应的ATTR_ID,DATA_SEG_STATE
				select ATTR_ID,DATA_SEG_STATE into temp_attr_id,temp_dataSeg_status from DM_DATA_SEG where DATA_SEG=temp_data_seg;
				-- 检查地市是否对应
				SELECT D.REGION_CODE INTO cyc_region_code FROM dm_data_seg s,dm_hlr_link h,dm_hlr_link r,dm_region D
				WHERE D.REGION_NAME=r.ATTR_NAME and h.parent_id=r.attr_id and s.attr_id= h.attr_id and s.DATA_SEG =temp_data_seg;
				if temp_region_code<>cyc_region_code then
					temp_errMeaasge := '该号段['||temp_data_seg||']归属地市有误，来源['||temp_region_code||'],数据管理系统中['||cyc_region_code||'],两者不一致!';
					--处理错误数据及对应数量
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
					delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num;
					commit;
					temp_errCount := temp_count;
					temp_succCount := 0;
					EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
					update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
						CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
					-- 删除回收共享表中已处理的数据
					delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
					commit;
					goto dataSegCycles;
				end if;
				-- 检查号段状态是否是质疑状态
				if temp_dataSeg_status=DM_PKG_CONST.DATA_SEG_STATE_NG THEN
					temp_errMeaasge :='该号段['||temp_data_seg||']是质疑数据，不能回收!';
					--处理错误数据及对应数量
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
					delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num;
					commit;
					temp_errCount := temp_count;
					temp_succCount := 0;
					EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
					update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
						CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
					-- 删除回收共享表中已处理的数据
					delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
					commit;
					goto dataSegCycles;
				END if;
				-- 根据号段获取IMSI段前10位（普通号段）或IMSI的11段列表（195号段）
				DM_PKG_DATA_SEG.IMSISegGenerate(temp_data_seg, temp_imsiSeg, retid, retmsg);
				if retid<>0 THEN
					temp_errMeaasge :='该号段['||temp_data_seg||']是没有IMSI规则，不能回收!';
					--处理错误数据及对应数量
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
					delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num;
					commit;
					temp_errCount := temp_count;
					temp_succCount := 0;
					EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
					update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
						CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
					-- 删除回收共享表中已处理的数据
					delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
					commit;
					goto dataSegCycles;
				END if;
				dbms_output.put_line(12);
				-- 号段状态为部分使用或未使用情况
				if temp_dataSeg_status=DM_PKG_CONST.DATA_SEG_STATE_UNUSED or temp_dataSeg_status=DM_PKG_CONST.DATA_SEG_STATE_USED THEN
					if substr(temp_data_seg,1,3) = '195' then
						temp_i := temp_i+1;
						DM_PKG_UTILS.stringToArray2(temp_imsiSeg,results,lenght);
						<<imsicursor2>>
						while temp_i <= lenght loop
							temp_imsiSeg11 := results(temp_i);
							select count(*) into temp_flag from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and substr(t.imsi,1,11)=temp_imsiSeg11;
							if temp_flag=0 then
								temp_i := temp_i+1;
								goto imsicursor2;
							end if;

							select t.status, t.imsi_count into temp_imsi_status,temp_imsi_count from dm_imsi t where t.imsi_seg=temp_imsiSeg11;
							-- 判断IMSI能否回收
							if temp_imsi_status=DM_PKG_CONST.IMSI_STATE_UNUSED THEN
								-- IMSI段未使用
								temp_errMeaasge :='该IMSI段['||temp_imsiSeg11||']未生成过数据，不能回收!';
								--处理错误数据及对应数量
								temp_err_str_copy := temp_err_str || ' and substr(t.imsi,1,11)=:5';
								EXECUTE IMMEDIATE temp_err_str_copy USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num,temp_imsiSeg11;
								delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and substr(t.imsi,1,11)=temp_imsiSeg11;
								commit;
								temp_errCount := temp_errCount + temp_flag;
								temp_succCount := temp_succCount - temp_flag;
								if temp_succCount=0 then
									EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
									-- 若可回收数据为0，更新批次状态和数量
									update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
										CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
									-- 删除回收共享表中已处理的数据
									delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
									commit;
									goto dataSegCycles;
								end if;
							elsif temp_imsi_status=DM_PKG_CONST.IMSI_STATE_USED THEN
								-- IMSI段部分使用,处理超出范围数据
								temp_imsi_index := 10000 - temp_imsi_count;
								temp_imsi_min := temp_imsiSeg11 || substr(DM_PKG_UTILS.padding(temp_imsi_index, '0', 5, 'L'),2,5);
								temp_imsi_max := temp_imsiSeg11 || '9999';
								select count(*) into temp_flag from DM_IMSI_CYC_TEMP t WHERE t.seq_num=temp_seq_num and t.imsi>=temp_imsi_min and t.imsi<=temp_imsi_max;
								IF temp_flag > 0 THEN
									temp_errMeaasge :='数据超出该IMSI段['||temp_imsiSeg11||']已分配数据范围，不能回收!';
									--处理错误数据及对应数量
									temp_err_str_copy := temp_err_str || ' and t.imsi>=:5 and t.imsi<=:6';
									EXECUTE IMMEDIATE temp_err_str_copy USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num,temp_imsi_min,temp_imsi_max;
									delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and t.imsi>=temp_imsi_min and t.imsi<=temp_imsi_max;
									commit;
									temp_errCount := temp_errCount + temp_flag;
									temp_succCount := temp_succCount - temp_flag;
									if temp_succCount=0 then
										EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
										-- 若可回收数据为0，更新批次状态和数量
										update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
											CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
										-- 删除回收共享表中已处理的数据
										delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
										commit;
										goto dataSegCycles;
									end if;
								END IF;
							END if;
						temp_i := temp_i+1;
						end loop imsicursor2;

					else
						-- 其他普通号段，循环10个IMSI段
						<<imsicursor>>
						while temp_i <= 9 loop
							temp_imsiSeg11 := temp_imsiSeg || TO_CHAR(temp_i);
							select count(*) into temp_flag from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and substr(t.imsi,1,11)=temp_imsiSeg11;
							if temp_flag=0 then
								temp_i := temp_i+1;
								goto imsicursor;
							end if;

							select t.status, t.imsi_count into temp_imsi_status,temp_imsi_count from dm_imsi t where t.imsi_seg=temp_imsiSeg11;
							-- 判断IMSI能否回收
							if temp_imsi_status=DM_PKG_CONST.IMSI_STATE_UNUSED THEN
								-- IMSI段未使用
								temp_errMeaasge :='该IMSI段['||temp_imsiSeg11||']未生成过数据，不能回收!';
								--处理错误数据及对应数量
								temp_err_str_copy := temp_err_str || ' and substr(t.imsi,1,11)=:5';
								EXECUTE IMMEDIATE temp_err_str_copy USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num,temp_imsiSeg11;
								delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and substr(t.imsi,1,11)=temp_imsiSeg11;
								commit;
								temp_errCount := temp_errCount + temp_flag;
								temp_succCount := temp_succCount - temp_flag;
								if temp_succCount=0 then
									EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
									-- 若可回收数据为0，更新批次状态和数量
									update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
										CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
									-- 删除回收共享表中已处理的数据
									delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
									commit;
									goto dataSegCycles;
								end if;
							elsif temp_imsi_status=DM_PKG_CONST.IMSI_STATE_USED THEN
								-- IMSI段部分使用,处理超出范围数据
								temp_imsi_index := 10000 - temp_imsi_count;
								temp_imsi_min := temp_imsiSeg11 || substr(DM_PKG_UTILS.padding(temp_imsi_index, '0', 5, 'L'),2,5);
								temp_imsi_max := temp_imsiSeg11 || '9999';
								select count(*) into temp_flag from DM_IMSI_CYC_TEMP t WHERE t.seq_num=temp_seq_num and t.imsi>=temp_imsi_min and t.imsi<=temp_imsi_max;
								IF temp_flag > 0 THEN
									temp_errMeaasge :='数据超出该IMSI段['||temp_imsiSeg11||']已分配数据范围，不能回收!';
									--处理错误数据及对应数量
									temp_err_str_copy := temp_err_str || ' and t.imsi>=:5 and t.imsi<=:6';
									EXECUTE IMMEDIATE temp_err_str_copy USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num,temp_imsi_min,temp_imsi_max;
									delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and t.imsi>=temp_imsi_min and t.imsi<=temp_imsi_max;
									commit;
									temp_errCount := temp_errCount + temp_flag;
									temp_succCount := temp_succCount - temp_flag;
									if temp_succCount=0 then
										EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
										-- 若可回收数据为0，更新批次状态和数量
										update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
											CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
										-- 删除回收共享表中已处理的数据
										delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
										commit;
										goto dataSegCycles;
									end if;
								END IF;
							END if;
							temp_i := temp_i+1;
						end loop imsicursor;
					end if;

				end if;
				-- 检查数据是否属于同一网段
				select h.hlr_code into temp_hlr_code from DM_HLR h,dm_hlr_link l where l.attr_name=h.hlr_name and l.attr_id=temp_attr_id and h.region_code=temp_region_code;
				select count(*) into temp_flag from DM_IMSI_CYC_TEMP t where t.SEQ_NUM=temp_seq_num and t.HLR_CODE=temp_hlr_code;
				if temp_flag<>temp_count then
					temp_errMeaasge := '3位网段不正确';
					--处理错误数据及对应数量
					temp_err_str := temp_err_str || ' and t.HLR_CODE<>' || temp_hlr_code;
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
					delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and t.HLR_CODE<>temp_hlr_code;
					commit;
					temp_errCount := temp_errCount + (temp_count-temp_flag);
					temp_succCount := temp_succCount - (temp_count-temp_flag);
					if temp_succCount=0 then
						EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
						-- 若可回收数据为0，更新批次状态和数量
						update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
							CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
						-- 删除回收共享表中已处理的数据
						delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
						commit;
						goto dataSegCycles;
					end if;
				end if;

				-- 检查数据中IMSI段是否一致
				if substr(temp_data_seg,1,3) = '195' then
						temp_str := 'select count(*) from DM_IMSI_CYC_TEMP t where t.SEQ_NUM=:1 and substr(t.IMSI,1,11) in (' || temp_imsiSeg ||  ')';
						EXECUTE IMMEDIATE temp_str INTO temp_flag USING temp_seq_num;

						if temp_flag<>temp_succCount then
							temp_errMeaasge := '号段与IMSI段关系不正确';

							--处理错误数据及对应数量
							temp_i := 1;
							DM_PKG_UTILS.stringToArray2(temp_imsiSeg,results,lenght);
							while temp_i <= lenght loop
								temp_imsiSeg11 := results(temp_i);
								temp_imsiSeg_err := temp_imsiSeg_err || ' and substr(t.IMSI,1,11)<>' || temp_imsiSeg11;
								temp_i := temp_i+1;
							end loop;

							temp_err_str := temp_err_str || temp_imsiSeg_err;
							EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
							temp_err_str_del := 'delete from DM_IMSI_CYC_TEMP t where t.seq_num=:1 ' || temp_imsiSeg_err;
							EXECUTE IMMEDIATE temp_err_str_del USING temp_seq_num;
							commit;
							temp_errCount := temp_errCount + (temp_succCount-temp_flag);
							temp_succCount := temp_flag;
							if temp_succCount=0 then
								EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
								-- 若可回收数据为0，更新批次状态和数量
								update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
									CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
								-- 删除回收共享表中已处理的数据
								delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
								commit;
								goto dataSegCycles;
							end if;
						end if;

				else
					select count(*) into temp_flag from DM_IMSI_CYC_TEMP t where t.SEQ_NUM=temp_seq_num and substr(t.IMSI,1,10)=temp_imsiSeg;
					if temp_flag<>temp_succCount then
						temp_errMeaasge := '号段与IMSI段关系不正确';
						--处理错误数据及对应数量
						temp_err_str := temp_err_str || ' and substr(t.IMSI,1,10)<>' || temp_imsiSeg;
						EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
						delete from DM_IMSI_CYC_TEMP t where t.seq_num=temp_seq_num and substr(t.IMSI,1,10)<>temp_imsiSeg;
						commit;
						temp_errCount := temp_errCount + (temp_succCount-temp_flag);
						temp_succCount := temp_flag;
						if temp_succCount=0 then
							EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
							-- 若可回收数据为0，更新批次状态和数量
							update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
								CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
							-- 删除回收共享表中已处理的数据
							delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
							commit;
							goto dataSegCycles;
						end if;
					end if;
				end if;

				-- 检查回收数据中是否有重复IMSI
				select count(distinct(IMSI)) into temp_flag from DM_IMSI_CYC_TEMP where seq_num=temp_seq_num;
				if temp_flag<>temp_succCount then
					temp_errMeaasge := '该号段下IMSI数据重复！';
					--查询多余的数量
					select count(*) into temp_flag from (select imsi FROM DM_IMSI_CYC_TEMP E WHERE seq_num=temp_seq_num AND E.ROWID > (SELECT MIN(x.ROWID)
					FROM DM_IMSI_CYC_TEMP x WHERE x.seq_num=temp_seq_num AND x.Imsi = e.Imsi));
					--处理错误数据及对应数量
					temp_err_str := temp_err_str || ' AND t.ROWID > (SELECT MIN(x.ROWID) FROM DM_IMSI_CYC_TEMP x WHERE x.seq_num=:5 AND x.imsi = t.imsi) ';
					EXECUTE IMMEDIATE temp_err_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num,temp_seq_num;
					delete from DM_IMSI_CYC_TEMP E WHERE E.ROWID > (SELECT MIN(X.ROWID) FROM DM_IMSI_CYC_TEMP X WHERE seq_num=temp_seq_num AND X.Imsi = E.Imsi);
					commit;
					temp_errCount := temp_errCount + temp_flag;
					temp_succCount := temp_succCount - temp_flag;
					if temp_succCount=0 then
						EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
						-- 若可回收数据为0，更新批次状态和数量
						update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
							CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
						-- 删除回收共享表中已处理的数据
						delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
						commit;
						goto dataSegCycles;
					end if;
				end if;
				temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;
				-- 检查IMSI是否有重复（与地市表比较）
				select count(distinct IMSI) into temp_flag from DM_IMSI_CYC_TEMP where seq_num=temp_seq_num;
				temp_str := 'SELECT COUNT(*) FROM '|| temp_table_name || ' a, DM_IMSI_CYC_TEMP b WHERE b.seq_num=:1 and a.IMSI=b.imsi';
				EXECUTE IMMEDIATE temp_str INTO temp_flag USING temp_seq_num;
				if temp_flag>0 then
					temp_errMeaasge := '回收数据与地市表数据重复';
					--处理错误数据及对应数量
					temp_str := 'INSERT INTO DM_SYNC_IMSI_CYC_ERR_REOCRD (SEQ_NUM,REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,OPRATE_TIME) ';
					temp_str := temp_str || 'SELECT b.SEQ_NUM,b.REGION_CODE,b.HLR_CODE,b.DATA_SEG,b.IMSI,:1,b.GENERATE_TIME,:2,:3 FROM '|| temp_table_name || ' a, DM_IMSI_CYC_TEMP b WHERE b.seq_num=:4 and a.IMSI=b.imsi ';
					EXECUTE IMMEDIATE temp_str USING DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_3,temp_errMeaasge,sysdate,temp_seq_num;
					temp_err_str2 := 'delete from DM_IMSI_CYC_TEMP t where t.imsi=(select t.imsi from '||temp_table_name||' b where t.seq_num=:1 and t.imsi=b.imsi)';
					EXECUTE IMMEDIATE temp_err_str2 USING temp_seq_num;
					commit;
					temp_errCount := temp_errCount + temp_flag;
					temp_succCount := temp_succCount - temp_flag;
					if temp_succCount=0 then
						EXECUTE IMMEDIATE temp_err2_str USING temp_seq_num;
						-- 若可回收数据为0，更新批次状态和数量
						update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_4,
							CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=0 where SEQ_NUM=temp_seq_num;
						-- 删除回收共享表中已处理的数据
						delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
						commit;
						goto dataSegCycles;
					end if;
				end if;

				-- IMSI回收数据从临时表导入正式表中
				temp_str := 'insert into ' || temp_table_name || ' (SEQ_NUM,REGION_CODE,HLR_CODE,ATTR_ID,DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES) ';
				temp_str := temp_str || 'select SEQ_NUM,REGION_CODE,HLR_CODE,' || temp_attr_id || ',DATA_SEG,IMSI,STATUS,BATCH_NUM,GENERATE_TIME,NOTES from DM_IMSI_CYC_TEMP where SEQ_NUM=:1';
				execute immediate temp_str using temp_seq_num;
				-- 更新批次表状态
				update DM_IMSI_CYC_BATCH set ATTR_ID=temp_attr_id,CYC_BATCH_STAUTS=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_1,
					CYC_ERRCOUNT=temp_errCount,CYC_SUCCCOUNT=temp_succCount where SEQ_NUM=temp_seq_num;
				-- 删除IMSI临时表中已转入正式表中的数据
				delete from DM_IMSI_CYC_TEMP where SEQ_NUM=temp_seq_num;
				commit;
				-- 错误数据复制到BOSS共享错误表中
				INSERT INTO DM_SYNC_IMSI_CYC_ERROR (REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,OPRATE_TIME)
					SELECT REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,sysdate FROM DM_SYNC_IMSI_CYC_ERR_REOCRD WHERE seq_num=temp_seq_num;
				-- 删除回收共享表中已回收的数据
				delete from DM_SYNC_IMSI_CYC t where t.region_code=temp_region_code and t.DATA_SEG=temp_data_seg and t.status=DM_PKG_CONST.IMSI_AUTO_CYC_STATUS_1;
				commit;
			END LOOP;
			end if;
			--关闭游标
			close temp_cursor;
		END LOOP;
		end if;
		--关闭游标
		close temp_region_cursor;

		retid	:= 0;
		retmsg	:= '回收IMSI导入成功。';

		-- 系统异常处理
		exception
			when others then
				rollback;
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end IMSIAutoCycImport;

	procedure IMSICycStatistics(strAttrId in number, strDataSeg in varchar2, iPage in number, iPageSize in number, results out cursorType, retid out number, retmsg out varchar2,  records out number)
	is
		temp_flag			number := 0;
		temp_region_short	VARCHAR2(20) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_str			VARCHAR2(1024) :='';
    temp_sql_recodes			VARCHAR2(1024) :='';
		temp_minPage number(11) := -1;
		temp_maxPage number(11) := -1;
	begin
		temp_minPage := ((iPage - 1) * iPageSize) + 1;
		temp_maxPage := iPage * iPageSize;

		-- 检查ATTR_ID是否存在
		select count(*) into temp_flag from DM_HLR_LINK where attr_id= strAttrId;
		if temp_flag<1 then
			retid	:= 8010;
			retmsg	:= 'ATTR_ID['|| strAttrId || ']对应的HLR信息不存在，请确认!';
			return;
		end if;

		-- 检查地市信息是否存在
		select count(*) into temp_flag from DM_HLR_LINK a, DM_HLR_LINK b, DM_REGION c
		where a.attr_id=strAttrId and a.parent_id=b.attr_id and b.attr_name=c.region_name;
		if temp_flag<1 then
			retid	:= 8011;
			retmsg	:= 'ATTR_ID['|| strAttrId || ']对应的地市信息不存在，请确认!';
			return;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_HLR_LINK a, DM_HLR_LINK b, DM_REGION c
		where a.attr_id=strAttrId and a.parent_id=b.attr_id and b.attr_name=c.region_name;

		-- 设置IMSI回收表名称
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		if strDataSeg is null or strDataSeg='' or length(strDataSeg)<>7 then
			-- 查询ATTR_ID对应的号段及未使用剩余量信息，分页显示
			temp_str := 'select * from (';
			temp_str := temp_str || 'select data_seg, imsi_count,ROWNUM as NUM from (';
			temp_str := temp_str || 'select a.data_seg as data_seg, count(*) as imsi_count from '|| temp_table_name ||' a ';
			temp_str := temp_str || 'where a.attr_id='|| strAttrId ||' and STATUS= '|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED || ' and a.data_seg in (select b.data_seg from DM_DATA_SEG b where b.attr_id='|| strAttrId ||') ';
			temp_str := temp_str || 'group by a.data_seg ';
			temp_str := temp_str || 'order by imsi_count desc))';
			temp_str := temp_str || ' where NUM between '||temp_minPage||' and '||temp_maxPage;
			temp_sql_recodes := 'select COUNT(distinct data_seg)  from '|| temp_table_name ||' a' || ' where a.attr_id='|| strAttrId ||' and STATUS= '|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED || ' and a.data_seg in (select b.data_seg from DM_DATA_SEG b where b.attr_id='|| strAttrId ||') ';
		elsif length(strDataSeg)=7 then
			-- 查询指定号段下的IMSI剩余量
			temp_str := 'select '|| strDataSeg ||' as data_seg, count(*) as imsi_count from '|| temp_table_name ||' a ';
			temp_str := temp_str || 'where a.attr_id='|| strAttrId ||' and a.data_seg='|| strDataSeg ||' and STATUS= '|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED || ' and a.data_seg in (select b.data_seg from DM_DATA_SEG b where b.attr_id='|| strAttrId ||') ';
			temp_sql_recodes := 'select COUNT(distinct data_seg)  from '|| temp_table_name ||' a' || ' where a.attr_id='|| strAttrId ||' and a.data_seg='|| strDataSeg ||' and STATUS= '|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED || ' and a.data_seg in (select b.data_seg from DM_DATA_SEG b where b.attr_id='|| strAttrId ||') ';
		end if;


		-- 执行
		open results for temp_str;
    execute immediate temp_sql_recodes into records;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end IMSICycStatistics;

	procedure GenerateIMSICycICCID(strAttrId in number, strDataSeg in varchar2, strFactoryCode in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2,retid out number, retmsg out varchar2)
	is
		temp_flag 		number := -1;
		temp_table_name	VARCHAR2(50) :='';
		temp_str		VARCHAR2(1024) :='';
		temp_region_short	VARCHAR2(20) :='';
		temp_iccid_1_6 	VARCHAR2(6) := null;
		temp_iccid_7 	VARCHAR2(1) := null;
		temp_iccid_8 	VARCHAR2(1) := null;
		temp_iccid_9_10 VARCHAR2(2) := null;
		temp_iccid_11_12 VARCHAR2(2) := null;
		temp_iccid_13 	VARCHAR2(1) := null;
		temp_iccid_14_20 VARCHAR2(7) := null;
		--temp_factory_code VARCHAR2(2) := null;
		temp_count number := 0;
	begin
		-- 检查地市信息是否存在
		select count(*) into temp_flag from DM_HLR_LINK a, DM_HLR_LINK b, DM_REGION c
		where a.attr_id=strAttrId and a.parent_id=b.attr_id and b.attr_name=c.region_name;
		if temp_flag<1 then
			retid	:= 8012;
			retmsg	:= 'ATTR_ID['|| strAttrId || ']对应的地市信息不存在，请确认!';
			return;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_HLR_LINK a, DM_HLR_LINK b, DM_REGION c
		where a.attr_id=strAttrId and a.parent_id=b.attr_id and b.attr_name=c.region_name;

		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 判断号段表中是否存在该号段数据
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG = strDataSeg;
		if temp_flag < 1 then
			retid	:= 8013;
			retmsg	:= '号段表中不存在该号段！';
			return;
		end if;
		-- 判断回收表中是否存在该号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where DATA_SEG = '||strDataSeg;
		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8014;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
			return;
		end if;

		-- 判断输入数量是否超过IMSI剩余量
		if temp_flag < iCount then
			retid	:= 8015;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，小于输入的数量[' || iCount || ']。';
			return;
		end if;

		-- 判断卡商是否正确
		select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = strFactoryCode;
		if temp_flag < 1 then
			retid	:= 8016;
			retmsg	:= '输入的卡商不存在！';
			return;
		end if;
		--select FACTORY_CODE into temp_factory_code from DM_FACTORY where FACTORY_NAME = strFactoryName;

		-- 生成ICCID前6位
		if (substr(strDataSeg,1,2)= '13' and substr(strDataSeg,3,1) between '4' and '9') or
			(substr(strDataSeg,1,2)= '15' and substr(strDataSeg,3,1) between '0' and '2') or
			(substr(strDataSeg,1,2)= '15' and substr(strDataSeg,3,1) between '7' and '9') or
			(substr(strDataSeg,1,2)= '18' and substr(strDataSeg,3,1) between '7' and '8') or
			(substr(strDataSeg,1,3)= '182') or (substr(strDataSeg,1,3)= '147') then
				temp_iccid_1_6:= '898600';
		elsif (substr(strDataSeg,1,3)= '183') or
			(substr(strDataSeg,1,3)= '184') or
			(substr(strDataSeg,1,3)= '178') or
			(substr(strDataSeg,1,3)= '170') or   -- 20191111 Add
			(substr(strDataSeg,1,3)= '172') then -- 20191111 Add
				temp_iccid_1_6:= '898602';
			elsif (substr(strDataSeg,1,3)= '198') or -- 20191111 Add
			(substr(strDataSeg,1,3)= '165') or       -- 20191111 Add
			(substr(strDataSeg,1,3)= '195') then     -- 20191111 Add
				temp_iccid_1_6:= '898607';
		else
			retid	:= 8017;
			retmsg	:= '输入的号段不正确！';
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
		elsif substr(strDataSeg,1,3)= '170' then temp_iccid_7 := 'E'; -- 20191111 Add
		elsif substr(strDataSeg,1,3)= '172' then temp_iccid_7 := 'F'; -- 20191111 Add
		elsif substr(strDataSeg,1,3)= '198' then temp_iccid_7 := 'A'; -- 20191111 Add
		elsif substr(strDataSeg,1,3)= '165' then temp_iccid_7 := 'D'; -- 20191111 Add
		elsif substr(strDataSeg,1,3)= '195' then temp_iccid_7 := 'E'; -- 20191111 Add
		else
			retid	:= 8018;
			retmsg	:= '输入的号段不正确!';
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
		if temp_iccid_13 = 'F' then
			--卡商为远程写卡
			--如果temp_iccid_14_20为F999999时，已经达到最大值，报错
			if temp_iccid_14_20 = 'F999999' then
				retid := 8019;
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
				retid := 8020;
				retmsg := '分配数量过界,当前可用数量为:' || temp_count;
				return;
			end if;
			-- 组装起始ICCID
			strICCIDStart := temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13 || temp_iccid_14_20;
			-- 组装终止ICCID
			strICCIDEnd := substr(strICCIDStart,1,14) || substr(to_char(1000000 + to_number(substr(strICCIDStart,15,6)) + iCount - 1), 2, 6);

		else
			--卡商为非远程写卡
			--如果temp_iccid_14_20为9999999时，已经达到最大值，抱错
			if temp_iccid_14_20 = '9999999' then
				retid := 8021;
				retmsg := 'ICCID已经达到最大值!';
				return;
			end if;
			--如果temp_iccid_14_20为空时，设置新的ICCID从0000000开始,否则加1开始
			if temp_iccid_14_20 is null then temp_iccid_14_20 := '0000000';
			else temp_iccid_14_20 := substr((to_number(temp_iccid_14_20) + 10000001),2,7);
			end if;
			--判断输入的数量是否过界
			temp_count := 9999999 - to_number(temp_iccid_14_20) + 1;
			if temp_count < iCount then
				retid := 8022;
				retmsg := '分配数量过界,当前可用数量为:' || temp_count;
				return;
			end if;
			-- 组装起始ICCID
			strICCIDStart := temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13 || temp_iccid_14_20;
			-- 组装终止ICCID
			strICCIDEnd := substr(strICCIDStart,1,13) || substr(to_char(10000000 + to_number(substr(strICCIDStart,14,7)) + iCount - 1), 2, 7);

		end if;

		retid	:= 0;
		retmsg := '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg := sqlerrm;
	end GenerateIMSICycICCID;

	procedure IMSICycDataAssign(strAttrId in number, strBatchNum in varchar2,strFactoryCode in varchar2,strFactoryName in varchar2,strCardName in varchar2, strDataSeg in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2)
	is
		temp_batchNum	varchar2(12) := null;
		temp_smsp		varchar2(14) := null;
		temp_region_name varchar2(20) := null;
		temp_attr_name	varchar2(24) := null;
		temp_region_short varchar2(50) := null;
		temp_table_name	VARCHAR2(50) :='';
		temp_imsi		varchar2(100) := null;
		temp_imsi_seg	varchar2(11) := null;
		temp_iccid_start varchar2(20) := null;
		temp_iccid_end	varchar2(20) := null;
		temp_count		number := -1;
		temp_imsi_start	varchar2(15) := null;
		temp_imsi_end	varchar2(15) := null;
		temp_msisdn_start varchar2(11) := null;
		temp_msisdn_end	varchar2(11) := null;
		temp_flag		number := -1;
		temp_str		varchar2(1025) := null;
	begin
		temp_msisdn_start := strDataSeg || '0000';
		temp_msisdn_end := strDataSeg || '0000';

		-- 判断批次号是否正确,生成批次号和短信中心
		DM_PKG_DATA_ASSIGN.GenerateBatchInfoByAttrId(strAttrId,temp_batchNum,temp_smsp,temp_region_name,retid,retmsg);
		if retid <> 0 then
			-- 获取批次号失败，直接返回错误信息
			return;
		end if;
		if strBatchNum <> temp_batchNum then
			retid	:= 8023;
			retmsg	:= '输入的批次号不正确！';
			return;
		end if;
		-- 判断地市表中是否存在该地市信息
		select count(*) into temp_flag from DM_REGION where REGION_NAME=temp_region_name;
		if temp_flag < 1 then
			retid	:= 8024;
			retmsg	:= '地市表中不存在名称为[' || temp_region_name || ']的地市信息！';
			return;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		-- 设置IMSI回收表名
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 判断输入的卡商是否存在
		select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = strFactoryCode;
		if temp_flag < 1 then
			retid	:= 8025;
			retmsg	:= '名称为[' || strFactoryName || ']的卡商信息不存在！';
			return;
		end if;

		-- 判断输入的卡类型是否存在
		select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME=strCardName;
		if temp_flag < 1 then
			retid	:= 8026;
			retmsg	:= '指定卡类型[' || strCardName || ']不存在！';
			return;
		end if;

		-- 查询HLR名称
		select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=strAttrId;

		-- 判断号段是否存在
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
		if temp_flag < 1 then
			retid	:= 8027;
			retmsg	:= '要分配的号段在号段表中不存在。';
			return;
		end if;

		-- 获取IMSI段前10位
		DM_PKG_DATA_SEG.IMSISegGenerate(strDataSeg, temp_imsi, retid, retmsg);

		-- 判断回收表中是否存在该号段。
		if substr(strDataSeg,1,3) = '195' then
		  temp_str := 'select count(*) from '||temp_table_name||' where DATA_SEG = '||strDataSeg ||' and substr(IMSI,1,11) in(' || temp_imsi || ') and STATUS=0';
		else
		  temp_str := 'select count(*) from '||temp_table_name||' where DATA_SEG = '||strDataSeg ||' and substr(IMSI,1,10)=' || temp_imsi || ' and STATUS=0';
		end if;
		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8028;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
			return;
		end if;

		--判断输入数量是否超过IMSI剩余量
		if temp_flag < iBatchCount then
			retid	:= 8029;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，小于输入的数量[' || iBatchCount || ']。';
			return;
		end if;

		-- 判断批次表中号段对应的最小IMSI段
		if substr(strDataSeg,1,3) = '195' then
		  temp_str := 'select min(substr(imsi_start,1,11)) from DM_BATCH t where substr(imsi_start,1,11) in (' || temp_imsi || ')';
		else
		  temp_str := 'select min(substr(imsi_start,1,11)) from DM_BATCH t where imsi_start like ''' || temp_imsi || '%''';
		end if;
		execute immediate temp_str into temp_imsi_seg;
		temp_imsi_start := temp_imsi_seg || '0000';
		temp_imsi_end := temp_imsi_start;

		-- 判断起始和终止ICCID的前13位是否一致
		if substr(strICCIDStart, 1, 13) <> substr(strICCIDEnd, 1, 13) then
			retid	:= 8028;
			retmsg	:= '输入的起始和终止ICCID的前13位不一致！';
			return;
		end if;

		-- 判断起始ICCID是否合法
		GenerateIMSICycICCID(strAttrId, strDataSeg, strFactoryCode, iBatchCount, temp_iccid_start, temp_iccid_end, retid, retmsg);
		if retid <> 0 then
			-- 直接返回错误信息
			return;
		end if;
		if temp_iccid_start <> strICCIDStart then
			retid	:= 8029;
			retmsg	:= '输入的起始ICCID不正确！';
			return;
		end if;
		-- 判断终止ICCID是否合法
		if temp_iccid_end <> strICCIDEnd then
			retid	:= 8030;
			retmsg	:= '输入的终止ICCID不正确！';
			return;
		end if;

		-- 判断输入的ICCID区间是否与批次表中号段区间重叠
		if substr(strICCIDStart, 13, 1) = 'F' then 	--卡商为远程写卡
			select count(*) into temp_flag from DM_BATCH
				where substr(ICCID_START, 1, 14)=substr(strICCIDStart, 1, 14) and
				substr(ICCID_END, 15, 6)>=substr(strICCIDStart, 15, 6);
		else
			select count(*) into temp_flag from DM_BATCH
				where substr(ICCID_START, 1, 13)=substr(strICCIDStart, 1, 13) and
				substr(ICCID_END, 14, 7)>=substr(strICCIDStart, 14, 7);
		end if;
		if temp_flag > 0 then
			retid	:= 8031;
			retmsg	:= 'ICCID段重叠，请检查ICCID起始和终止值！';
			return;
		end if;

		-- 判断ICCID起止范围与批次数量是否相等
		if substr(strICCIDEnd, 13, 1) = 'F' then
			temp_count := to_number(substr(strICCIDEnd, -6, 6)) - to_number(substr(strICCIDStart, -6, 6)) + 1;
		else
			temp_count := to_number(substr(strICCIDEnd, -7, 7)) - to_number(substr(strICCIDStart, -7, 7)) + 1;
		end if;

		if temp_count <> iBatchCount then
			retid := 8032;
			retmsg := 'ICCID起止范围与批次数量不匹配！';
			return;
		end if;


		-- 插入批次信息
		insert into DM_BATCH values(
					strBatchNum, 	-- BATCH_NUM
					temp_region_name, 	-- BATCH_REGION
					temp_smsp,		-- SMSP
					temp_attr_name,	-- HLR_NAME
					'未知',			-- BRAND_NAME,山东?
					strFactoryName,	-- FACTORY_NAME
					strCardName,	-- CARD_TYPE
					temp_msisdn_start,	-- MSISDN_START
					temp_msisdn_end,	-- MSISDN_END
					DM_PKG_CONST.IMSI_TYPE_REPLACE,	-- DATA_TYPE,山东固定
					iBatchCount,	-- BATCH_COUNT
					temp_imsi_start,	-- IMSI_START
					temp_imsi_end,		-- IMSI_END
					strICCIDStart,	-- ICCID_START
					strICCIDEnd,	-- ICCID_END
					sysdate,		-- BATCH_TIME
					'0',			-- BATCH_STATUS
					'0',			-- PRODUCE_STATUS
					'0',			-- ORDER_CODE
					strNotes, -- NOTES
          strFactoryCode);		-- FACTORY_CODE

		-- 更新IMSI回收表的批次号和状态
		if substr(strDataSeg,1,3) = '195' then
		  temp_str := 'update '||temp_table_name||' a set a.BATCH_NUM='''||strBatchNum||''', a.STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_USED ||' where a.DATA_SEG='||strDataSeg||' and a.IMSI in ';
		  temp_str := temp_str || '(select IMSI from (';
		  temp_str := temp_str || 'select c.IMSI,ROWNUM as NUM from (';
		  temp_str := temp_str || 'select * from '||temp_table_name||' b where DATA_SEG = '||strDataSeg ||' and substr(IMSI,1,11) in(' || temp_imsi || ') and STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED ||' order by IMSI';
		  temp_str := temp_str || ') c) where NUM between 0 and ' || iBatchCount || ')';
		else
		  temp_str := 'update '||temp_table_name||' a set a.BATCH_NUM='''||strBatchNum||''', a.STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_USED ||' where a.DATA_SEG='||strDataSeg||' and a.IMSI in ';
		  temp_str := temp_str || '(select IMSI from (';
		  temp_str := temp_str || 'select c.IMSI,ROWNUM as NUM from (';
		  temp_str := temp_str || 'select * from '||temp_table_name||' b where DATA_SEG = '||strDataSeg ||' and substr(IMSI,1,10)=' || temp_imsi || ' and STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED ||' order by IMSI';
		  temp_str := temp_str || ') c) where NUM between 0 and ' || iBatchCount || ')';
		end if;
		execute immediate temp_str;

		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycDataAssign;


	procedure IMSICycXhDataAssign(strRegionShort in varchar2, strXHDataSeg in varchar2, strBatchNum in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2)
	is
		temp_batchNum	varchar2(20) := null;
		temp_smsp		varchar2(14) := null;
		temp_region_code varchar2(20) :=null;
		temp_region_name varchar2(20) := null;
		temp_attr_name	varchar2(24) := null;
		temp_region_short varchar2(5) := null;
		temp_table_name	VARCHAR2(50) :='';
		temp_factory_name	VARCHAR2(50) :='';
		temp_imsi		varchar2(100) := null;
		temp_imsi_seg	varchar2(11) := null;
		temp_iccid_start varchar2(20) := null;
		temp_iccid_end	varchar2(20) := null;
		temp_count		number := -1;
		temp_imsi_start	varchar2(15) := null;
		temp_imsi_end	varchar2(15) := null;
		temp_msisdn_start varchar2(11) := null;
		temp_msisdn_end	varchar2(11) := null;
		temp_flag		number := -1;
		temp_str		varchar2(2048) := null;
	begin
		temp_msisdn_start := strXHDataSeg || '0000';
		temp_msisdn_end := strXHDataSeg || '0000';

		-- 判断批次号是否正确
    	DM_PKG_XH_DATA_MNG.XHBatchInfoGenerate(strRegionShort, temp_batchNum,temp_smsp,retid,retmsg);
		if retid <> 0 then
			-- 获取批次号失败，直接返回错误信息
			return;
		end if;
		if strBatchNum <> temp_batchNum then
			retid	:= 8023;
			retmsg	:= '输入的批次号不正确！';
			return;
		end if;
		-- 判断地市表中是否存在该地市信息
		select count(*) into temp_flag from DM_REGION where REGION_SHORT=strRegionShort;
		if temp_flag < 1 then
			retid	:= 8024;
			retmsg	:= '地市表中不存在缩写为[' || strRegionShort || ']的地市信息！';
			return;
		end if;

		-- 判断批次号时已经验证地市信息的存在性
    	select REGION_NAME,SMSP,REGION_CODE into temp_region_name,temp_smsp,temp_region_code from DM_REGION where REGION_SHORT=strRegionShort;

		-- 设置IMSI回收表名
		temp_table_name := 'DM_IMSI_CYC_XH';

		-- 判断输入的卡商是否存在
		select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = strFactoryName;
		if temp_flag < 1 then
			retid	:= 8025;
			retmsg	:= '代码为[' || strFactoryName || ']的卡商信息不存在！';
			return;
		end if;

		select FACTORY_NAME into temp_factory_name from DM_FACTORY where FACTORY_CODE = strFactoryName;

		-- 判断输入的卡类型是否存在
		select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME=strCardName;
		if temp_flag < 1 then
			retid	:= 8026;
			retmsg	:= '指定卡类型[' || strCardName || ']不存在！';
			return;
		end if;

		-- 判断携号网段表中是否存在该网段信息
	    select count(*) into temp_flag from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg, 1, 3);
	    if temp_flag < 1 then
	      retid := 8014;
	      retmsg  := '携号网段表中不存在网段[' || substr(strXHDataSeg, 1, 3) || ']信息！';
	      return;
	    end if;

		-- 获取IMSI段前10位
		DM_PKG_XH_DATA_MNG.XHIMSISegGenerate('1300', temp_imsi, retid, retmsg);

		-- 判断回收表中是否存在该号段。
		temp_str := 'select count(*) from '||temp_table_name||' where DATA_SEG = '||strXHDataSeg ||' and substr(IMSI,1,8)=' || substr(temp_imsi, 1,8) || ' and STATUS=0';

		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8028;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
			return;
		end if;

		--判断输入数量是否超过IMSI剩余量
		if temp_flag < iBatchCount then
			retid	:= 8029;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，小于输入的数量[' || iBatchCount || ']。';
			return;
		end if;

		-- 判断批次表中号段对应的最小IMSI段
		temp_str := 'select min(substr(imsi_start,1,11)) from DM_BATCH t where imsi_start like ''' || substr(temp_imsi, 1,8) || '%''';

		execute immediate temp_str into temp_imsi_seg;
		temp_imsi_start := temp_imsi_seg || '0000';
		temp_imsi_end := temp_imsi_start;

		-- 判断起始和终止ICCID的前13位是否一致
		if substr(strICCIDStart, 1, 13) <> substr(strICCIDEnd, 1, 13) then
			retid	:= 8028;
			retmsg	:= '输入的起始和终止ICCID的前13位不一致！';
			return;
		end if;

		-- 判断起始ICCID是否合法
    	DM_PKG_XH_DATA_MNG.XHICCIDSegGenerate('1300', temp_factory_name, iBatchCount, temp_iccid_start, temp_iccid_end, retid, retmsg);

		if retid <> 0 then
			-- 直接返回错误信息
			return;
		end if;
		if temp_iccid_start <> strICCIDStart then
			retid	:= 8029;
			retmsg	:= '输入的起始ICCID不正确！';
			return;
		end if;
		-- 判断终止ICCID是否合法
		if temp_iccid_end <> strICCIDEnd then
			retid	:= 8030;
			retmsg	:= '输入的终止ICCID不正确！';
			return;
		end if;

		-- 判断携号ICCID起止范围与批次数量是否相等
	    temp_count := to_number(substr(strICCIDEnd, -7, 7)) - to_number(substr(strICCIDStart, -7, 7)) + 1;
	    if temp_count <> iBatchCount then
	      retid  := 8032;
	      retmsg := '携号ICCID起止范围与批次数量不匹配！';
	      return;
	    end if;

		-- 判断输入的ICCID区间是否与批次表中号段区间重叠
		select count(*) into temp_flag from DM_XH_BATCH
	      where substr(ICCID_START, 1, 13)=substr(strICCIDStart, 1, 13)
	      and to_number(substr(ICCID_END, -7, 7))>=to_number(substr(strICCIDStart, -7, 7));
	    if temp_flag > 0 then
	      retid := 8037;
	      retmsg  := '携号ICCID段重叠，请检查携号ICCID起始和终止值！';
	      return;
	    end if;

		if temp_count <> iBatchCount then
			retid := 8032;
			retmsg := 'ICCID起止范围与批次数量不匹配！';
			return;
		end if;


		-- 插入批次信息
	    insert into DM_XH_BATCH values(
	          strBatchNum,  -- BATCH_NUM
	          temp_region_name,   -- BATCH_REGION 集团
	          temp_smsp,    -- SMSP
	          substr(strXHDataSeg,1,3), -- HLR_NAME
	          '未知',     -- BRAND_NAME
	          temp_factory_name, -- FACTORY_NAME
	          strCardName,  -- CARD_TYPE
	          temp_msisdn_start,  -- MSISDN_START
	          temp_msisdn_start,  -- MSISDN_END
	          strDataType,  -- DATA_TYPE
	          iBatchCount,  -- BATCH_COUNT
	          strIMSIStart, -- IMSI_START
	          strIMSIEnd,   -- IMSI_END
	          strICCIDStart,  -- ICCID_START
	          strICCIDEnd,  -- ICCID_END
	          sysdate,    -- BATCH_TIME
	          '0',      -- BATCH_STATUS
	          '0',      -- PRODUCE_STATUS
	          '0',      -- ORDER_CODE
	          strNotes);    -- NOTES

		-- 更新IMSI回收表的批次号和状态
		temp_str := 'update '||temp_table_name||' a set a.BATCH_NUM='''||strBatchNum||''', a.STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_USED ||' where a.DATA_SEG='||strXHDataSeg||' and a.IMSI in ';
		  temp_str := temp_str || '(select IMSI from (';
		  temp_str := temp_str || 'select c.IMSI,ROWNUM as NUM from (';
		  temp_str := temp_str || 'select * from '||temp_table_name||' b where region_code = '''|| temp_region_code ||''' and substr(IMSI,1,8)=' || substr(temp_imsi, 1,8) || ' and STATUS='|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED ||' order by IMSI';
		  temp_str := temp_str || ') c) where NUM between 0 and ' || iBatchCount || ')';


		execute immediate temp_str;

		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycXhDataAssign;

	procedure IMSICycBatchDel(strBatchNum in varchar2, retid out number, retmsg out varchar2)
	is
		temp_flag			number := -1;
		temp_notes			varchar2(100) := null;
		temp_region_name	varchar2(16) := null;
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_str			varchar2(1025) := null;
		temp_batchStatus	number := -1;
		temp_produceStatus	number := -1;
		temp_count			number := -1;
	begin
		-- 找不到批次号对应的信息
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM = strBatchNum;
		if temp_flag < 1 then
			retid	:= 8033;
			retmsg	:= '指定的批次号[' || strBatchNum || ']信息不存在！';
			return;
		end if;

		-- 获取批次信息
		select BATCH_REGION,BATCH_COUNT,BATCH_STATUS,PRODUCE_STATUS,NOTES into temp_region_name,temp_count,temp_batchStatus,temp_produceStatus, temp_notes from DM_BATCH where BATCH_NUM = strBatchNum;

		-- 检查要删除的批次是否属于IMSI回收批次
		if substr(temp_notes,1,4) <> DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8034;
			retmsg	:= '要删除的批次不是IMSI回收批次！';
			return;
		end if;

		-- 查询地市，并设置IMSI回收表名
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 删除的批次已经生成订单（实体卡或两不一快）
		if temp_batchStatus = DM_PKG_CONST.BACTH_STATE_GENERATE then
			retid	:= 8035;
			retmsg	:= '删除的批次已经生成订单！';
			return;
		end if;

		-- 删除的批次已经生成数据（远程写卡）
		if temp_produceStatus = DM_PKG_CONST.DATA_STATE_GENERATE then
			retid	:= 8036;
			retmsg	:= '删除的批次已经生成数据！';
			return;
		end if;

		-- 查询IMSI回收表中批次对应的数据量
		temp_str := 'select count(*) from '|| temp_table_name || ' where BATCH_NUM=:1 and STATUS=:2';
		execute immediate temp_str into temp_flag
			using strBatchNum,DM_PKG_CONST.IMSI_CYC_STATUS_USED;

		-- 判断要删除的批次对应的IMSI数据在回收表中是否存在
		if temp_flag < 1 then
			retid	:= 8037;
			retmsg	:= '删除的批次对应的数据，在IMSI回收表中不存在！';
			return;
		end if;

		-- 判断要删除的批次对应的IMSI数据量与批次数量是否一致
		if temp_flag <> temp_count then
			retid	:= 8038;
			retmsg	:= '删除的批次对应的IMSI数量与批次数量不一致！';
			return;
		end if;

		-- 删除批次信息
		delete from DM_BATCH where BATCH_NUM = strBatchNum;

		-- 更新IMSI回收表中的数据状态，并把批次号设置为空
		temp_str := 'update '|| temp_table_name || ' set STATUS=:1,BATCH_NUM='''' where BATCH_NUM=:2';
		execute immediate temp_str
			using DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED,strBatchNum;

		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycBatchDel;

	procedure XHCycBatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2)
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
	  temp_table_name   VARCHAR2(50) :='';
	  temp_str      varchar2(1025) := null;
	begin
	  -- 找不到输入的批次号对应的批次
	  select count(*) into temp_flag from DM_XH_BATCH where BATCH_NUM = strBatchNum;
	  if temp_flag < 1 then
	    retid := 8037;
	    retmsg  := '指定的携号批次号[' || strBatchNum || ']信息不存在！';
	    return;
	  end if;

	  select BATCH_COUNT into temp_flag from DM_XH_BATCH where BATCH_NUM = strBatchNum;

	  -- 是否已生成数据
	  select PRODUCE_STATUS into temp_procuce_states from DM_XH_BATCH where BATCH_NUM = strBatchNum;
	  if temp_procuce_states = DM_PKG_CONST.XH_DATA_STATE_GENERATE then
	    retid := 8042;
	    retmsg  := '指定的携号批次号[' || strBatchNum || ']信息已生成数据，不能删除批次！';
	    return;
	  end if;

	  temp_table_name := 'DM_IMSI_CYC_XH';

	  -- 查询IMSI回收表中批次对应的数据量
	  temp_str := 'select count(*) from '|| temp_table_name || ' where BATCH_NUM=:1 and STATUS=:2';
	  execute immediate temp_str into temp_count
	    using strBatchNum,DM_PKG_CONST.IMSI_CYC_STATUS_USED;

	  -- 判断要删除的批次对应的IMSI数据在回收表中是否存在
	  if temp_count < 1 then
	    retid := 8037;
	    retmsg  := '删除的批次对应的数据，在IMSI回收表中不存在！';
	    return;
	  end if;

	  -- 判断要删除的批次对应的IMSI数据量与批次数量是否一致
	  if temp_flag <> temp_count then
	    retid := 8038;
	    retmsg  := '删除的批次对应的IMSI数量与批次数量不一致！';
	    return;
	  end if;

	  -- 更新IMSI回收表中的数据状态，并把批次号设置为空
	  temp_str := 'update '|| temp_table_name || ' set STATUS=:1,BATCH_NUM='''' where BATCH_NUM=:2';
	  execute immediate temp_str
	    using DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED,strBatchNum;

	  -- 删除携号批次信息
	  delete from DM_XH_BATCH where BATCH_NUM = strBatchNum;

	  commit;

	  retid := 0;
	  retmsg  := '成功!';

	  -- 系统异常处理
	  exception
	    when others then
	      retid := sqlcode;
	      retmsg  := sqlerrm;
	      rollback;
	end XHCycBatchInfoDelete;

	procedure IMSICycDataGenForRPS(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_cursor 		cursorType := null;
		temp_batch_num		number := 0;
		temp_iccid_start	VARCHAR2(20) :='';
		temp_iccid_end		VARCHAR2(20) :='';
		temp_iccid			VARCHAR2(20) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_smsp			VARCHAR2(14) :='';
		temp_pin1			VARCHAR2(4) :='';
		temp_pin2			VARCHAR2(4) :='';
		temp_puk1			VARCHAR2(8) :='';
		temp_puk2			VARCHAR2(8) :='';
		temp_k				VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_opc			VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_region_name	VARCHAR2(16) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_kind_id		number := 0;
		temp_batch_type		VARCHAR2(100) :='';
		temp_order_num		VARCHAR2(9) :='';
		temp_i				number := 0;
		temp_status			number := 0;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
	begin
		-- 批次表中查询是否存在批次信息
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 8039;
			retmsg	:= '找不到批次['|| strBatchNum || ']信息!';
			return;
		end if;

		-- 查询批次信息
		select BATCH_COUNT,BATCH_REGION, substr(MSISDN_START,1,7), ICCID_START, ICCID_END, SMSP, NOTES,PRODUCE_STATUS
		into temp_batch_count,temp_region_name, temp_data_seg,temp_iccid_start,temp_iccid_end, temp_smsp, temp_batch_type,temp_status
		from DM_BATCH where BATCH_NUM=strBatchNum;

		-- 检查批次类型
		if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8040;
			retmsg	:= '该批次不属于IMSI回收类型，请确认!';
			return;
		end if;

		-- 判断该批次是否已生成数据
		if temp_status=DM_PKG_CONST.DATA_STATE_GENERATE then
			retid	:= 8041;
			retmsg	:= '该批次已生成过数据，不能重复生成，请确认!';
			return;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 判断号段表中是否存在该号段数据
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG = temp_data_seg;
		if temp_flag < 1 then
			retid	:= 8042;
			retmsg	:= '号段表中不存在该号段！';
			return;
		end if;

		-- 判断回收表中是否存在该批次及号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8043;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
			return;
		end if;

		--判断输入数量与已分配的数量是否一致
		if temp_flag <> temp_batch_count then
			retid	:= 8044;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，不等于输入的数量[' || temp_batch_count || ']。';
			return;
		end if;

		-- 检查ICCID起止段与数量是否匹配
		temp_batch_num := TO_NUMBER(substr(temp_iccid_end,15,6)) - TO_NUMBER(substr(temp_iccid_start,15,6)) +1;
		if temp_batch_num<>temp_batch_count then
			retid	:= 8045;
			retmsg	:= '批次表中的数量与ICCID起止段数量不相同!';
			return;
		end if;

		-- 查询回收IMSI数据
		temp_str := 'select IMSI from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		temp_str := temp_str || ' order by IMSI';
		open temp_cursor for temp_str;

		temp_kind_id := DM_PKG_CONST.KIND_ID;
		temp_pin1 := DM_PKG_CONST.PIN1;

		-- 先清空表
		delete from DM_IMSI_CYC_RPM_TEMP;
		commit;

		-- 生成个性化数据
		if temp_cursor %isopen and 0<=temp_i and temp_i< temp_batch_num then
			loop
				-- 回收IMSI
				fetch temp_cursor into temp_imsi;
				exit when temp_cursor %notfound;

				-- 生成PIN2
				temp_pin2 := floor(DBMS_RANDOM.VALUE(0,9999));
				if temp_pin2 < 0 or temp_pin2 > 9999 then
					retid	:= 8046;
					retmsg	:= '产生PIN2失败!';
					rollback;
					return;
				end if;
				temp_pin2 := DM_PKG_UTILS.padding(temp_pin2, '0', 4, 'L');

				-- 生成PUK1
				temp_puk1 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk1 < 0 or temp_puk1 > 99999999 then
					retid	:= 8047;
					retmsg	:= '产生PUK1失败!';
					rollback;
					return;
				end if;
				temp_puk1 := DM_PKG_UTILS.padding(temp_puk1, '0', 8, 'L');

				-- 生成PUK2
				temp_puk2 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk2 < 0 or temp_puk2 > 99999999 then
					retid	:= 8048;
					retmsg	:= '产生PUK2失败!';
					rollback;
					return;
				end if;
				temp_puk2 := DM_PKG_UTILS.padding(temp_puk2, '0', 8, 'L');

				-- ICCID
				temp_iccid := substr(temp_iccid_end,1,14) || substr(TO_CHAR(1000000 + TO_NUMBER(substr(temp_iccid_start,15,6)) + temp_i ), 2,6);

				-- 插入个性化数据到DM_IMSI_CYC_RPM_TEMP表
				insert into DM_IMSI_CYC_RPM_TEMP values(temp_order_num, strBatchNum, temp_imsi, temp_iccid, temp_pin1, temp_pin2, temp_puk1, temp_puk2, temp_smsp, temp_k, temp_opc, temp_kind_id, '', '');
				temp_i := temp_i + 1;

				-- 更新IMSI回收表中的数据状态为 2：已生成
				temp_str := 'update '|| temp_table_name || ' set STATUS=:1, GENERATE_TIME=sysdate where IMSI=:2';
				execute immediate temp_str
					using DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE,temp_imsi;

			end loop;
		end if;

		-- 把IMSI回收，已生成的数据移动到BACKUP表中
		temp_str := 'insert into DM_IMSI_CYC_BACKUP select * from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 从IMSI回收地市表中删除已生成的数据
		temp_str := 'delete from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 更新批次表中数据生成状态
		update DM_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_GENERATE where BATCH_NUM=strBatchNum;

		-- 提交
		commit;

		open results for
			select * from DM_IMSI_CYC_RPM_TEMP where BATCH_NUM=strBatchNum order by IMSI;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycDataGenForRPS;

	-----------------------------------------------------------------------------------------
	-- 名称：XHIMSICycDataGenForRPS
	-- 功能：IMSI回收-数据生成(远程写卡数据)
	-- 调用：{ DM_PKG_IMSI_CYC.XHIMSICycDataGenForRPS(?,?,?,?) }
	-- 参数：
	--	strBatchNum		out	number				批次号
	--	results			out	cursorType			生成的数据列表
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure XHIMSICycDataGenForRPS(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_cursor 		cursorType := null;
		temp_batch_num		number := 0;
		temp_iccid_start	VARCHAR2(20) :='';
		temp_iccid_end		VARCHAR2(20) :='';
		temp_iccid			VARCHAR2(20) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_smsp			VARCHAR2(14) :='';
		temp_pin1			VARCHAR2(4) :='';
		temp_pin2			VARCHAR2(4) :='';
		temp_puk1			VARCHAR2(8) :='';
		temp_puk2			VARCHAR2(8) :='';
		temp_k				VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_opc			VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_region_name	VARCHAR2(16) :='';
		temp_region_code	VARCHAR2(10) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_kind_id		number := 0;
		temp_batch_type		VARCHAR2(100) :='';
		temp_order_num		VARCHAR2(9) :='';
		temp_i				number := 0;
		temp_status			number := 0;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
	begin
		-- 批次表中查询是否存在批次信息
		select count(*) into temp_flag from DM_XH_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 8039;
			retmsg	:= '找不到批次['|| strBatchNum || ']信息!';
			return;
		end if;

		-- 查询批次信息
		select BATCH_COUNT,BATCH_REGION, substr(MSISDN_START,1,7), ICCID_START, ICCID_END, SMSP, NOTES,PRODUCE_STATUS
		into temp_batch_count,temp_region_name, temp_data_seg,temp_iccid_start,temp_iccid_end, temp_smsp, temp_batch_type,temp_status
		from DM_XH_BATCH where BATCH_NUM=strBatchNum;

		-- 检查批次类型
		if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8040;
			retmsg	:= '该批次不属于IMSI回收类型，请确认!';
			return;
		end if;

		-- 判断该批次是否已生成数据
		if temp_status=DM_PKG_CONST.DATA_STATE_GENERATE then
			retid	:= 8041;
			retmsg	:= '该批次已生成过数据，不能重复生成，请确认!';
			return;
		end if;

		-- 查询地市代码
		select region_code into temp_region_code from DM_REGION where region_name = temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_XH';

		-- 判断回收表中是否存在该批次及号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and region_code = '''||temp_region_code ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8043;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该批次！';
			return;
		end if;

		--判断输入数量与已分配的数量是否一致
		if temp_flag <> temp_batch_count then
			retid	:= 8044;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，不等于输入的数量[' || temp_batch_count || ']。';
			return;
		end if;

		-- 检查ICCID起止段与数量是否匹配
		temp_batch_num := TO_NUMBER(substr(temp_iccid_end,15,6)) - TO_NUMBER(substr(temp_iccid_start,15,6)) +1;
		if temp_batch_num<>temp_batch_count then
			retid	:= 8045;
			retmsg	:= '批次表中的数量与ICCID起止段数量不相同!';
			return;
		end if;

		-- 查询回收IMSI数据
		temp_str := 'select IMSI from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and region_code = '''||temp_region_code ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		temp_str := temp_str || ' order by IMSI';
		open temp_cursor for temp_str;

		temp_kind_id := DM_PKG_CONST.KIND_ID;
		temp_pin1 := DM_PKG_CONST.PIN1;

		-- 先清空表
		delete from DM_IMSI_CYC_RPM_TEMP;
		commit;

		-- 生成个性化数据
		if temp_cursor %isopen and 0<=temp_i and temp_i< temp_batch_num then
			loop
				-- 回收IMSI
				fetch temp_cursor into temp_imsi;
				exit when temp_cursor %notfound;

				-- 生成PIN2
				temp_pin2 := floor(DBMS_RANDOM.VALUE(0,9999));
				if temp_pin2 < 0 or temp_pin2 > 9999 then
					retid	:= 8046;
					retmsg	:= '产生PIN2失败!';
					rollback;
					return;
				end if;
				temp_pin2 := DM_PKG_UTILS.padding(temp_pin2, '0', 4, 'L');

				-- 生成PUK1
				temp_puk1 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk1 < 0 or temp_puk1 > 99999999 then
					retid	:= 8047;
					retmsg	:= '产生PUK1失败!';
					rollback;
					return;
				end if;
				temp_puk1 := DM_PKG_UTILS.padding(temp_puk1, '0', 8, 'L');

				-- 生成PUK2
				temp_puk2 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk2 < 0 or temp_puk2 > 99999999 then
					retid	:= 8048;
					retmsg	:= '产生PUK2失败!';
					rollback;
					return;
				end if;
				temp_puk2 := DM_PKG_UTILS.padding(temp_puk2, '0', 8, 'L');

				-- ICCID
				temp_iccid := substr(temp_iccid_end,1,14) || substr(TO_CHAR(1000000 + TO_NUMBER(substr(temp_iccid_start,15,6)) + temp_i ), 2,6);

				-- 插入个性化数据到DM_IMSI_CYC_RPM_TEMP表
				insert into DM_IMSI_CYC_RPM_TEMP values(temp_order_num, strBatchNum, temp_imsi, temp_iccid, temp_pin1, temp_pin2, temp_puk1, temp_puk2, temp_smsp, temp_k, temp_opc, temp_kind_id, '', '');
				temp_i := temp_i + 1;

				-- 更新IMSI回收表中的数据状态为 2：已生成
				temp_str := 'update '|| temp_table_name || ' set STATUS=:1, GENERATE_TIME=sysdate where IMSI=:2';
				execute immediate temp_str
					using DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE,temp_imsi;

			end loop;
		end if;

		-- 把IMSI回收，已生成的数据移动到BACKUP表中
		temp_str := 'insert into DM_IMSI_CYC_BACKUP select seq_num, region_code, region_code as hlr_code, attr_id, data_seg, imsi, status, batch_num, generate_time, notes from '|| temp_table_name || ' where BATCH_NUM=:1 and REGION_CODE =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_region_code,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 从IMSI回收地市表中删除已生成的数据
		temp_str := 'delete from '|| temp_table_name || ' where BATCH_NUM=:1 and REGION_CODE =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_region_code,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 更新批次表中数据生成状态
		update DM_XH_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_GENERATE where BATCH_NUM=strBatchNum;

		-- 提交
		commit;

		open results for
			select * from DM_IMSI_CYC_RPM_TEMP where BATCH_NUM=strBatchNum order by IMSI;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end XHIMSICycDataGenForRPS;

	-----------------------------------------------------------------------------------------
	-- 名称：IMSICycDataRollback
	-- 功能：IMSI回收-数据回滚(远程写卡数据/实体卡/两不一快共用)
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycDataRollback(?,?,?) }
	-- 参数：
	--	strBatchNum		out	number				批次号
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure IMSICycDataRollback(strBatchNum in varchar2, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_data_seg		VARCHAR2(7) :='';
		temp_region_name	VARCHAR2(16) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_batch_type		VARCHAR2(100) :='';
		temp_batchStatus	number := -1;
		temp_produceStatus	number := -1;
		temp_batch_time		date;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
	begin
		-- 批次表中查询是否存在批次信息
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 8049;
			retmsg	:= '找不到批次['|| strBatchNum || ']信息!';
			return;
		end if;

		-- 查询批次信息
		select BATCH_COUNT,BATCH_REGION, substr(MSISDN_START,1,7), NOTES,BATCH_STATUS,PRODUCE_STATUS,BATCH_TIME
		into temp_batch_count,temp_region_name, temp_data_seg, temp_batch_type,temp_batchStatus,temp_produceStatus,temp_batch_time
		from DM_BATCH where BATCH_NUM=strBatchNum;

		-- 检查批次类型
		if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8050;
			retmsg	:= '该批次不属于IMSI回收类型，请确认!';
			return;
		end if;

		-- 判断该批次是否已生成数据
		if temp_produceStatus<>DM_PKG_CONST.DATA_STATE_GENERATE and temp_batchStatus<>DM_PKG_CONST.BACTH_STATE_DATA_GEN then
			-- 判断该批次是否已生成数据
			if temp_produceStatus<>DM_PKG_CONST.DATA_STATE_GENERATE then
				retid	:= 8051;
				retmsg	:= '该批次未生成数据，不能回滚，请确认!';
				return;
			end if;

			-- 判断该批次是否已生成数据
			if temp_batchStatus<>DM_PKG_CONST.BACTH_STATE_DATA_GEN then
				retid	:= 8052;
				retmsg	:= '该批次对应的订单未生成IMSI回收数据，不能回滚，请确认!';
				return;
			end if;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		-- 判断号段表中是否存在该号段数据
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG = temp_data_seg;
		if temp_flag < 1 then
			retid	:= 8053;
			retmsg	:= '号段表中不存在该号段！';
			return;
		end if;

		-- 判断回收表中是否存在该批次及号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		execute immediate temp_str into temp_flag;
		if temp_flag > 0 then
			retid	:= 8054;
			retmsg	:= '回收表['||temp_table_name||']中已存在该号段，不能回滚！';
			return;
		end if;

		--判断回滚表中是否存在该批次数据
		select count(*) into temp_flag from DM_IMSI_CYC_BACKUP where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			select round(to_number(sysdate-temp_batch_time)) into temp_flag from dual;
			if temp_flag > DM_PKG_CONST.IMSI_CYC_BACKUP_DAYS then
				retid	:= 8055;
				retmsg	:= '回滚表中数据已超过['||DM_PKG_CONST.IMSI_CYC_BACKUP_DAYS||']天，已删除。';
				return;
			else
				retid	:= 8056;
				retmsg	:= '回滚表中不存在批次号为['||strBatchNum||']的数据。';
				return;
			end if;
		end if;

		--判断批次数量与回滚数量是否一致
		if temp_flag <> temp_batch_count then
			retid	:= 8057;
			retmsg	:= '回收表中IMSI数量为['||temp_flag||']，批次表中的数量为[' || temp_batch_count || ']。';
			return;
		end if;

		-- 把BACKUP数据移动到IMSI回收表中
		temp_str := 'insert into '|| temp_table_name || ' select * from DM_IMSI_CYC_BACKUP where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 从IBACKUP中删除回滚的数据
		delete from DM_IMSI_CYC_BACKUP where BATCH_NUM=strBatchNum and DATA_SEG =temp_data_seg and STATUS=DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 更新IMSI回收表中的数据状态为 1：已使用
		temp_str := 'update '|| temp_table_name || ' set STATUS=:1, GENERATE_TIME='''' where BATCH_NUM=:2';
		execute immediate temp_str
			using DM_PKG_CONST.IMSI_CYC_STATUS_USED,strBatchNum;

		-- 更新批次表中数据生成状态
		if temp_produceStatus=DM_PKG_CONST.DATA_STATE_GENERATE and temp_batchStatus=DM_PKG_CONST.BACTH_STATE_INIT then
			-- 如果是远程写卡批次
			update DM_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT where BATCH_NUM=strBatchNum;
		elsif temp_produceStatus=DM_PKG_CONST.DATA_STATE_INIT and temp_batchStatus=DM_PKG_CONST.BACTH_STATE_DATA_GEN then
			-- 如果是实卡订单或两不一快批次
			update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.BACTH_STATE_GENERATE where BATCH_NUM=strBatchNum;

			-- 检查订单类型
			select count(distinct(b.BATCH_STATUS)) into temp_flag from DM_BATCH a, DM_BATCH b
				where a.BATCH_NUM=strBatchNum and a.order_code=b.order_code;
			-- 若同一订单下所有批次已回滚，则回滚订单状态
			if temp_flag=1 then
				select distinct(b.BATCH_STATUS) into temp_batchStatus from DM_BATCH a, DM_BATCH b
					where a.BATCH_NUM=strBatchNum and a.order_code=b.order_code;
				if temp_batchStatus=DM_PKG_CONST.BACTH_STATE_GENERATE  then
					update DM_ORDER a set a.ORDER_STATUS=DM_PKG_CONST.BACTH_STATE_GENERATE
						where a.ORDER_CODE=(select ORDER_CODE from DM_BATCH b where b.BATCH_NUM=strBatchNum);
				end if;
			end if;
		end if;

		-- 提交
		commit;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycDataRollback;

	-----------------------------------------------------------------------------------------
	-- 名称：XHIMSICycDataRollback
	-- 功能：IMSI回收-数据回滚(远程写卡数据/实体卡/两不一快共用)
	-- 调用：{ DM_PKG_IMSI_CYC.XHIMSICycDataRollback(?,?,?) }
	-- 参数：
	--	strBatchNum		out	number				批次号
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure XHIMSICycDataRollback(strBatchNum in varchar2, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_data_seg		VARCHAR2(7) :='';
		temp_region_name	VARCHAR2(16) :='';
		temp_region_code	VARCHAR2(10) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_batch_type		VARCHAR2(100) :='';
		temp_batchStatus	number := -1;
		temp_produceStatus	number := -1;
		temp_batch_time		date;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
	begin
		-- 批次表中查询是否存在批次信息
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 8049;
			retmsg	:= '找不到批次['|| strBatchNum || ']信息!';
			return;
		end if;

		-- 查询批次信息
		select BATCH_COUNT,BATCH_REGION, substr(MSISDN_START,1,7), NOTES,BATCH_STATUS,PRODUCE_STATUS,BATCH_TIME
		into temp_batch_count,temp_region_name, temp_data_seg, temp_batch_type,temp_batchStatus,temp_produceStatus,temp_batch_time
		from DM_XH_BATCH where BATCH_NUM=strBatchNum;

		-- 检查批次类型
		if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8050;
			retmsg	:= '该批次不属于IMSI回收类型，请确认!';
			return;
		end if;

		-- 判断该批次是否已生成数据
		if temp_produceStatus<>DM_PKG_CONST.DATA_STATE_GENERATE and temp_batchStatus<>DM_PKG_CONST.BACTH_STATE_DATA_GEN then
			-- 判断该批次是否已生成数据
			if temp_produceStatus<>DM_PKG_CONST.DATA_STATE_GENERATE then
				retid	:= 8051;
				retmsg	:= '该批次未生成数据，不能回滚，请确认!';
				return;
			end if;

			-- 判断该批次是否已生成数据
			if temp_batchStatus<>DM_PKG_CONST.BACTH_STATE_DATA_GEN then
				retid	:= 8052;
				retmsg	:= '该批次对应的订单未生成IMSI回收数据，不能回滚，请确认!';
				return;
			end if;
		end if;

		-- 查询地市缩写
		select REGION_SHORT,REGION_CODE into temp_region_short,temp_region_code from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_XH';

		-- 判断回收表中是否存在该批次及号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and REGION_CODE = '''||temp_region_code ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		execute immediate temp_str into temp_flag;
		if temp_flag > 0 then
			retid	:= 8054;
			retmsg	:= '回收表['||temp_table_name||']中已存在该号段，不能回滚！';
			return;
		end if;

		--判断回滚表中是否存在该批次数据
		select count(*) into temp_flag from DM_IMSI_CYC_BACKUP where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			select round(to_number(sysdate-temp_batch_time)) into temp_flag from dual;
			if temp_flag > DM_PKG_CONST.IMSI_CYC_BACKUP_DAYS then
				retid	:= 8055;
				retmsg	:= '回滚表中数据已超过['||DM_PKG_CONST.IMSI_CYC_BACKUP_DAYS||']天，已删除。';
				return;
			else
				retid	:= 8056;
				retmsg	:= '回滚表中不存在批次号为['||strBatchNum||']的数据。';
				return;
			end if;
		end if;

		--判断批次数量与回滚数量是否一致
		if temp_flag <> temp_batch_count then
			retid	:= 8057;
			retmsg	:= '回收表中IMSI数量为['||temp_flag||']，批次表中的数量为[' || temp_batch_count || ']。';
			return;
		end if;

		-- 把BACKUP数据移动到IMSI回收表中
		temp_str := 'insert into '|| temp_table_name || ' select * from DM_IMSI_CYC_BACKUP where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 从IBACKUP中删除回滚的数据
		delete from DM_IMSI_CYC_BACKUP where BATCH_NUM=strBatchNum and DATA_SEG =temp_data_seg and STATUS=DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 更新IMSI回收表中的数据状态为 1：已使用
		temp_str := 'update '|| temp_table_name || ' set STATUS=:1, GENERATE_TIME='''' where BATCH_NUM=:2';
		execute immediate temp_str
			using DM_PKG_CONST.IMSI_CYC_STATUS_USED,strBatchNum;

		-- 更新批次表中数据生成状态
		if temp_produceStatus=DM_PKG_CONST.DATA_STATE_GENERATE and temp_batchStatus=DM_PKG_CONST.BACTH_STATE_INIT then
			-- 如果是远程写卡批次
			update DM_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_INIT where BATCH_NUM=strBatchNum;
		end if;

		-- 提交
		commit;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end XHIMSICycDataRollback;

	-----------------------------------------------------------------------------------------
	-- 名称：IMSICycDataGenForOrder
	-- 功能：IMSI回收-数据生成(实卡订单-单个批次)
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycDataGenForOrder(?,?,?,?,?) }
	-- 参数：
	--	strOrderNum		out	number				订单号
	--	strBatchNum		out	number				批次号
	--	results			out	cursorType			生成的数据列表
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure IMSICycDataGenForOrder(strOrderNum in varchar2, strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_cursor 		cursorType := null;
		temp_batch_num		number := 0;
		temp_iccid_start	VARCHAR2(20) :='';
		temp_iccid_end		VARCHAR2(20) :='';
		temp_iccid			VARCHAR2(20) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_smsp			VARCHAR2(14) :='';
		temp_pin1			VARCHAR2(4) :='';
		temp_pin2			VARCHAR2(4) :='';
		temp_puk1			VARCHAR2(8) :='';
		temp_puk2			VARCHAR2(8) :='';
		temp_k				VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_opc			VARCHAR2(32) :=''; -- 实卡订单时由java生成
		temp_region_name	VARCHAR2(16) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_kind_id		number := 0;
		temp_batch_type		VARCHAR2(100) :='';
		temp_i				number := 0;
		temp_status			number := 0;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
	begin
		-- 批次表中查询是否存在批次信息
		select count(*) into temp_flag from DM_BATCH where BATCH_NUM=strBatchNum;
		if temp_flag < 1 then
			retid	:= 8058;
			retmsg	:= '找不到批次['|| strBatchNum || ']信息!';
			return;
		end if;

		-- 查询批次信息
		select BATCH_COUNT,BATCH_REGION, substr(MSISDN_START,1,7), ICCID_START, ICCID_END, SMSP, NOTES,PRODUCE_STATUS
		into temp_batch_count,temp_region_name, temp_data_seg,temp_iccid_start,temp_iccid_end, temp_smsp, temp_batch_type,temp_status
		from DM_BATCH where BATCH_NUM=strBatchNum;
		-- 检查批次类型
		if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
			retid	:= 8059;
			retmsg	:= '该批次不属于IMSI回收类型，请确认!';
			return;
		end if;

		-- 判断该批次是否已生成数据
		if temp_status=DM_PKG_CONST.BACTH_STATE_DATA_GEN then
			retid	:= 8060;
			retmsg	:= '该批次已生成过数据，不能重复生成，请确认!';
			return;
		end if;

		-- 查询地市缩写
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;
		-- 判断号段表中是否存在该号段数据
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG = temp_data_seg;
		if temp_flag < 1 then
			retid	:= 8061;
			retmsg	:= '号段表中不存在该号段！';
			return;
		end if;
		-- 判断回收表中是否存在该批次及号段数据
		temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		execute immediate temp_str into temp_flag;
		if temp_flag < 1 then
			retid	:= 8062;
			retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
			return;
		end if;

		--判断批次数量与已分配的数量是否一致
		if temp_flag <> temp_batch_count then
			retid	:= 8063;
			retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，不等于输入的数量[' || temp_batch_count || ']。';
			return;
		end if;

		-- 检查ICCID起止段与数量是否匹配
		temp_batch_num := TO_NUMBER(substr(temp_iccid_end,14,7)) - TO_NUMBER(substr(temp_iccid_start,14,7)) +1;
		if temp_batch_num<>temp_batch_count then
			retid	:= 8064;
			retmsg	:= '批次表中的数量与ICCID起止段数量不相同!';
			return;
		end if;

		-- 查询回收IMSI数据
		temp_str := 'select IMSI from '||temp_table_name||' where BATCH_NUM=''' || strBatchNum || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
		temp_str := temp_str || ' order by IMSI';
		open temp_cursor for temp_str;

		temp_kind_id := DM_PKG_CONST.KIND_ID;
		temp_pin1 := DM_PKG_CONST.PIN1;

		-- 先清空表
		delete from DM_IMSI_CYC_RPM_TEMP;
		commit;

		-- 生成个性化数据
		if temp_cursor %isopen and 0<=temp_i and temp_i< temp_batch_num then
			loop
				-- 回收IMSI
				fetch temp_cursor into temp_imsi;
				exit when temp_cursor %notfound;

				-- 生成PIN2
				temp_pin2 := floor(DBMS_RANDOM.VALUE(0,9999));
				if temp_pin2 < 0 or temp_pin2 > 9999 then
					retid	:= 8065;
					retmsg	:= '产生PIN2失败!';
					rollback;
					return;
				end if;
				temp_pin2 := DM_PKG_UTILS.padding(temp_pin2, '0', 4, 'L');

				-- 生成PUK1
				temp_puk1 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk1 < 0 or temp_puk1 > 99999999 then
					retid	:= 8066;
					retmsg	:= '产生PUK1失败!';
					rollback;
					return;
				end if;
				temp_puk1 := DM_PKG_UTILS.padding(temp_puk1, '0', 8, 'L');

				-- 生成PUK2
				temp_puk2 := floor(DBMS_RANDOM.VALUE(0,99999999));
				if temp_puk2 < 0 or temp_puk2 > 99999999 then
					retid	:= 8067;
					retmsg	:= '产生PUK2失败!';
					rollback;
					return;
				end if;
				temp_puk2 := DM_PKG_UTILS.padding(temp_puk2, '0', 8, 'L');

				-- ICCID
				temp_iccid := substr(temp_iccid_end,1,13) || substr(TO_CHAR(10000000 + TO_NUMBER(substr(temp_iccid_start,14,7)) + temp_i ), 2,7);

				-- 插入个性化数据到DM_IMSI_CYC_RPM_TEMP表
				insert into DM_IMSI_CYC_RPM_TEMP values(strOrderNum, strBatchNum, temp_imsi, temp_iccid, temp_pin1, temp_pin2, temp_puk1, temp_puk2, temp_smsp, temp_k, temp_opc, temp_kind_id, '', '');
				temp_i := temp_i + 1;

				-- 更新IMSI回收表中的数据状态为 2：已生成
				temp_str := 'update '|| temp_table_name || ' set STATUS=:1 where IMSI=:2';
				execute immediate temp_str
					using DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE,temp_imsi;

			end loop;
		end if;

		-- 把IMSI回收，已生成的数据移动到BACKUP表中
		temp_str := 'insert into DM_IMSI_CYC_BACKUP select * from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 从IMSI回收地市表中删除已生成的数据
		temp_str := 'delete from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
		execute immediate temp_str
			using strBatchNum,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

		-- 更新批次表中数据生成状态
		update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.BACTH_STATE_DATA_GEN where BATCH_NUM=strBatchNum;
		-- 提交
		commit;
		open results for
			select * from DM_IMSI_CYC_RPM_TEMP where BATCH_NUM=strBatchNum order by IMSI;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycDataGenForOrder;

	-----------------------------------------------------------------------------------------
	-- 名称：IMSICycDataGenForLBYK
	-- 功能：IMSI回收-数据生成(实卡订单-单个批次)
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycDataGenForLBYK(?,?,?,?) }
	-- 参数：
	--	strOrderNum		out	number				订单号
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure IMSICycDataGenForLBYK(strOrderNum in varchar2, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_cursor 		cursorType := null;
		temp_batchList_cursor cursorType := null;
		temp_batch_num		number := 0;
		temp_batch_no		VARCHAR2(12) :='';
		temp_iccid_start	VARCHAR2(20) :='';
		temp_iccid_end		VARCHAR2(20) :='';
		temp_iccid			VARCHAR2(20) :='';
		temp_data_seg		VARCHAR2(7) :='';
		temp_imsi			VARCHAR2(15) :='';
		temp_smsp			VARCHAR2(14) :='';
		temp_pin1			VARCHAR2(4) :='';
		temp_pin2			VARCHAR2(4) :='';
		temp_puk1			VARCHAR2(8) :='';
		temp_puk2			VARCHAR2(8) :='';
		temp_region_name	VARCHAR2(16) :='';
		temp_region_short	VARCHAR2(2) :='';
		temp_table_name		VARCHAR2(50) :='';
		temp_kind_id		number := 0;
		temp_batch_type		VARCHAR2(100) :='';
		temp_i				number := 0;
		temp_status			number := 0;
		temp_str			VARCHAR2(1024) :='';
		temp_flag			number := 0;
		temp_order_count	number := 0;
		temp_order_type		VARCHAR2(100) :='';
		temp_batch_status	number := 0;
	begin
		-- 检查订单信息是否存在
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE=strOrderNum;
		if temp_flag<1 then
			retid	:= 8068;
			retmsg	:= '订单表中不存在该订单信息，请确认!';
			return;
		end if;

		-- 检查订单类型
		select order_count, notes into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
		if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC_LBYK then
			retid	:= 8069;
			retmsg	:= '该订单不属于IMSI回收两不一快订单，请确认!';
			return;
		end if;

		-- 检查订单数量
		select SUM(BATCH_COUNT) into temp_batch_count from DM_BATCH where ORDER_CODE=strOrderNum;
		if temp_order_count<>temp_batch_count then
			retid	:= 8070;
			retmsg	:= '订单表中的数量与批次表中的数量不相同!';
			return;
		end if;

		-- 判断输入批次厂商是否一致
		select count(distinct(FACTORY_NAME)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8071;
			retmsg	:= '生成订单批次的卡商不一致!';
			return;
		end if;

		-- 判断输入批次卡类型是否一致
		select count(distinct(CARD_TYPE)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8072;
			retmsg	:= '生成订单批次的卡类型不一致!';
			return;
		end if;

		-- 判断输入订单各批次订单生成状态是否一致
		select count(distinct(BATCH_STATUS)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8073;
			retmsg	:= '生成订单批次的订单生成状态不一致!';
			return;
		end if;

		-- 判断输入订单各批次订单生成状态是否是已生成订单且未生成IMSI回收数据
		select distinct(BATCH_STATUS) into temp_batch_status from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_batch_status <> DM_PKG_CONST.BACTH_STATE_GENERATE then
			retid	:= 8074;
			retmsg	:= '生成订单批次的订单生成状态不是已生成订单!';
			return;
		end if;

		--查询短消息中心
		select count(distinct b.SMSP) into temp_flag from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;
		if temp_flag < 1 then
			retid	:= 8075;
			retmsg	:= '找不到订单对应的短消息中心!';
			return;
		elsif temp_flag > 1 then
			retid	:= 8076;
			retmsg	:= '同一订单下的各批次对应的短消息中心不一致!';
			return;
		end if;
		select distinct(b.SMSP) into temp_smsp from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;

		--检查批次是否属于同一地市，同一网段
		select count(*) into temp_flag from (
			select distinct b.BATCH_REGION, b.HLR_NAME from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum
			);
		if temp_flag <> 1 then
			retid	:= 8077;
			retmsg	:= '该订单下的批次信息不属于同一地市、同一网段下!';
			return;
		end if;

		-- 查询地市缩写
		select distinct b.BATCH_REGION into temp_region_name from DM_ORDER a, DM_BATCH b where a.ORDER_CODE=b.ORDER_CODE and a.ORDER_CODE=strOrderNum;
		-- 设置IMSI回收地市表
		select REGION_SHORT into temp_region_short from DM_REGION where REGION_NAME=temp_region_name;
		temp_table_name := 'DM_IMSI_CYC_' || temp_region_short;

		temp_kind_id := DM_PKG_CONST.KIND_ID;
		temp_pin1 := DM_PKG_CONST.PIN1;

		-- 先清空表
		delete from DM_LBYK_RPM_TEMP;
		commit;

		-- 查询订单对应的批次信息
		open temp_batchList_cursor for
			select BATCH_NUM,BATCH_COUNT, substr(MSISDN_START,1,7), ICCID_START, ICCID_END, SMSP, NOTES,PRODUCE_STATUS
			from DM_BATCH where ORDER_CODE=strOrderNum;

		-- 遍历批次信息
		if temp_batchList_cursor %isopen then
			loop
				fetch temp_batchList_cursor
					into temp_batch_no, temp_batch_count, temp_data_seg,temp_iccid_start,temp_iccid_end, temp_smsp, temp_batch_type,temp_status;
				exit when temp_batchList_cursor %notfound;

				-- 初始化循环变量
				temp_i := 0;

				-- 检查批次类型
				if substr(temp_batch_type,1,4)<>DM_PKG_CONST.BATCH_TYPE_CYC then
					retid	:= 8078;
					retmsg	:= '该批次不属于IMSI回收类型，请确认!';
					rollback;
					return;
				end if;

				-- 判断该批次是否已生成数据
				if temp_status=DM_PKG_CONST.BACTH_STATE_DATA_GEN then
					retid	:= 8079;
					retmsg	:= '该批次已生成过数据，不能重复生成，请确认!';
					rollback;
					return;
				end if;

				-- 判断号段表中是否存在该号段数据
				select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG = temp_data_seg;
				if temp_flag < 1 then
					retid	:= 8080;
					retmsg	:= '号段表中不存在该号段！';
					rollback;
					return;
				end if;

				-- 判断回收表中是否存在该批次及号段数据
				temp_str := 'select count(*) from '||temp_table_name||' where BATCH_NUM=''' || temp_batch_no || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
				execute immediate temp_str into temp_flag;
				if temp_flag < 1 then
					retid	:= 8081;
					retmsg	:= '回收表['||temp_table_name||']中，不存在该号段！';
					rollback;
					return;
				end if;

				--判断批次数量与已分配的数量是否一致
				if temp_flag <> temp_batch_count then
					retid	:= 8082;
					retmsg	:= '回收表中的IMSI数量为['||temp_flag||']，不等于输入的数量[' || temp_batch_count || ']。';
					rollback;
					return;
				end if;

				-- 检查ICCID起止段与数量是否匹配
				temp_batch_num := TO_NUMBER(substr(temp_iccid_end,14,7)) - TO_NUMBER(substr(temp_iccid_start,14,7)) +1;
				if temp_batch_num<>temp_batch_count then
					retid	:= 8083;
					retmsg	:= '批次表中的数量与ICCID起止段数量不相同!';
					rollback;
					return;
				end if;

				-- 查询回收IMSI数据
				temp_str := 'select IMSI from '||temp_table_name||' where BATCH_NUM=''' || temp_batch_no || ''' and DATA_SEG = '''||temp_data_seg ||''' and STATUS=' || DM_PKG_CONST.IMSI_CYC_STATUS_USED;
				temp_str := temp_str || ' order by IMSI';
				open temp_cursor for temp_str;

				-- 生成个性化数据
				if temp_cursor %isopen and 0<=temp_i and temp_i< temp_batch_num then
					loop
						-- 回收IMSI
						fetch temp_cursor into temp_imsi;
						exit when temp_cursor %notfound;

						-- 生成PIN2
						temp_pin2 := floor(DBMS_RANDOM.VALUE(0,9999));
						if temp_pin2 < 0 or temp_pin2 > 9999 then
							retid	:= 8084;
							retmsg	:= '产生PIN2失败!';
							rollback;
							return;
						end if;
						temp_pin2 := DM_PKG_UTILS.padding(temp_pin2, '0', 4, 'L');

						-- 生成PUK1
						temp_puk1 := floor(DBMS_RANDOM.VALUE(0,99999999));
						if temp_puk1 < 0 or temp_puk1 > 99999999 then
							retid	:= 8085;
							retmsg	:= '产生PUK1失败!';
							rollback;
							return;
						end if;
						temp_puk1 := DM_PKG_UTILS.padding(temp_puk1, '0', 8, 'L');

						-- 生成PUK2
						temp_puk2 := floor(DBMS_RANDOM.VALUE(0,99999999));
						if temp_puk2 < 0 or temp_puk2 > 99999999 then
							retid	:= 8086;
							retmsg	:= '产生PUK2失败!';
							rollback;
							return;
						end if;
						temp_puk2 := DM_PKG_UTILS.padding(temp_puk2, '0', 8, 'L');

						-- ICCID
						temp_iccid := substr(temp_iccid_end,1,13) || substr(TO_CHAR(10000000 + TO_NUMBER(substr(temp_iccid_start,14,7)) + temp_i ), 2,7);

						-- 插入个性化数据到 DM_LBYK_RPM_TEMP 表
						insert into DM_LBYK_RPM_TEMP values(strOrderNum, temp_imsi, temp_iccid, temp_pin1, temp_pin2, temp_puk1, temp_puk2, temp_smsp, temp_kind_id, '', '');
						temp_i := temp_i + 1;

						-- 更新IMSI回收表中的数据状态为 2：已生成
						temp_str := 'update '|| temp_table_name || ' set STATUS=:1 where IMSI=:2';
						execute immediate temp_str
							using DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE,temp_imsi;

					end loop;
				end if;

				-- 把IMSI回收，已生成的数据移动到BACKUP表中
				temp_str := 'insert into DM_IMSI_CYC_BACKUP select * from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
				execute immediate temp_str
					using temp_batch_no,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

				-- 从IMSI回收地市表中删除已生成的数据
				temp_str := 'delete from '|| temp_table_name || ' where BATCH_NUM=:1 and DATA_SEG =:2 and STATUS=:3';
				execute immediate temp_str
					using temp_batch_no,temp_data_seg,DM_PKG_CONST.IMSI_CYC_STATUS_GENERATE;

				-- 更新批次表中数据生成状态
				update DM_BATCH set BATCH_STATUS=DM_PKG_CONST.BACTH_STATE_DATA_GEN where BATCH_NUM=temp_batch_no;

			end loop;
		end if;
		-- 提交
		commit;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
				rollback;
	end IMSICycDataGenForLBYK;

	-----------------------------------------------------------------------------------------
	-- 名称：GetBatchListByOrder
	-- 功能：IMSI回收-根据订单号获取所属的批次号列表
	-- 调用：{ DM_PKG_IMSI_CYC.GetBatchListByOrder(?,?,?) }
	-- 参数：
	--	strOrderNum		out	number				订单号
	--	iOrderCount		out	number				订单对应的数据量
	--	results			out	cursorType			订单号对应的批次号列表
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure GetBatchListByOrder(strOrderNum in varchar2, results out cursorType, detailInfo out cursorType, retid out number, retmsg out varchar2)
	is
		temp_batch_count	number := 0;
		temp_order_count	number := 0;
		temp_order_type		VARCHAR2(100) :='';
		temp_batch_status	number := 0;
		temp_flag			number := 0;
	begin
		-- 检查订单信息是否存在
		select count(*) into temp_flag from DM_ORDER where ORDER_CODE=strOrderNum;
		if temp_flag<1 then
			retid	:= 8087;
			retmsg	:= '订单表中不存在该订单信息，请确认!';
			return;
		end if;

		-- 检查订单类型
		select ORDER_COUNT,NOTES into temp_order_count, temp_order_type from DM_ORDER t where t.ORDER_CODE=strOrderNum;
		if temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC and temp_order_type<>DM_PKG_CONST.ORDER_TYPE_CYC_LBYK then
			retid	:= 8088;
			retmsg	:= '该订单即不属于IMSI回收实体卡订单，也不属于IMSI回收两不一快订单，请确认!';
			return;
		end if;

		-- 检查订单数量
		select SUM(BATCH_COUNT) into temp_batch_count from DM_BATCH where ORDER_CODE=strOrderNum;
		if temp_order_count<>temp_batch_count then
			retid	:= 8089;
			retmsg	:= '订单表中的数量与批次表中的数量不相同!';
			return;
		end if;

		-- 判断输入批次厂商是否一致
		select count(distinct(FACTORY_NAME)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8090;
			retmsg	:= '生成订单批次的卡商不一致!';
			return;
		end if;

		-- 判断输入批次卡类型是否一致
		select count(distinct(CARD_TYPE)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8091;
			retmsg	:= '生成订单批次的卡类型不一致!';
			return;
		end if;

		-- 检查订单信息
		select COUNT(*) into temp_flag from DM_ORDER a, DM_FACTORY b, DM_COMPANY c where a.ORDER_CODE=strOrderNum and a.FACTORY_NAME=b.FACTORY_NAME;
		if temp_flag < 1 then
			retid	:= 8092;
			retmsg	:= '指定的订单[' || strOrderNum || ']信息在卡商表或移动省公司表中不存在。';
			return;
		end if;

		-- 判断输入订单各批次订单生成状态是否一致
		select count(distinct(BATCH_STATUS)) into temp_flag from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_flag <> 1 then
			retid	:= 8093;
			retmsg	:= '生成订单批次的订单生成状态不一致!';
			return;
		end if;

		-- 判断输入订单各批次订单生成状态是否是已生成订单且未生成IMSI回收数据
		select distinct(BATCH_STATUS) into temp_batch_status from DM_BATCH where ORDER_CODE = strOrderNum;
		if temp_batch_status <> DM_PKG_CONST.BACTH_STATE_GENERATE then
			retid	:= 8094;
			retmsg	:= '生成订单批次的订单生成状态不是已生成订单!';
			return;
		end if;

		-- 查询订单对应的批次信息
		open results for
			select a.ORDER_CODE, a.FACTORY_NAME, a.CARD_TYPE, a.ORDER_COUNT, to_char(a.ORDER_TIME, 'yyyy-mm-dd hh24:mi:ss') as ORDER_TIME, a.ORDER_STATUS,a.NOTES,
				b.FACTORY_ID,b.FACTORY_CODE,b.CONTACT_PERSON as FAC_CON_PERSON,b.CONTACT_TEL as FAC_CON_TEL,b.FAX_NUM as FAC_CON_FAX,b.POST_NUM,b.POST_ADDR,
				c.COMPANY_NAME, c.CONTACT_PERSON as COM_CON_PERSON, c.CONTACT_TEL as COM_CON_TEL, c.FAX_NUM as COM_CON_FAX, c.E_MAIL
			from DM_ORDER a, DM_FACTORY b, DM_COMPANY c
			where a.ORDER_CODE=strOrderNum and a.FACTORY_NAME=b.FACTORY_NAME;

		open detailInfo for
			select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,
				ICCID_START,ICCID_END,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES
			from DM_BATCH where ORDER_CODE=strOrderNum order by BATCH_NUM;

		retid	:= 0;
		retmsg	:= '';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end GetBatchListByOrder;

	
	procedure GenerateEKiList(iNum in number, results out cursorType, retid out number, retmsg out varchar2)
	is
		temp_ki varchar2(32) := null;
		temp_batch varchar2(9) := '2G'||iNum;
	begin
		delete from DM_BLANK_CARD_TEMP;
		commit;
		-- 生成增强性KI
		for i in 1..iNum loop
			DM_PKG_BLANK_CARD_MNG.GenerateEnhancedKi(temp_ki, retid, retmsg);
			if temp_ki is null or length(temp_ki) <> 32 or retid <> 0 then
				retid	:= 8095;
				retmsg	:= '生成增强性Ki失败，Ki=['|| temp_ki || ']。';
				return;
			end if;

			insert into DM_BLANK_CARD_TEMP(BATCH_NUM,CARD_SN,K1,K,SN_TIME) values(temp_batch,i,i,temp_ki,sysdate);
		end loop;
		commit;

		open results for select K from DM_BLANK_CARD_TEMP where BATCH_NUM = temp_batch;

		delete from DM_BLANK_CARD_TEMP;
		commit;

		retid	:= 0;
		retmsg	:= '成功!';

		-- 系统异常处理
	exception
		when others then
			retid  := sqlcode;
			retmsg  := sqlerrm;
			rollback;
	end GenerateEKiList;

	-----------------------------------------------------------------------------------------
	-- IMSICycBatchList
	-- 功能：IMSI导入批次列表查询
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycBatchList(?,?,?,?,?,?) }
	-- 参数：
	--	strDataSeg		in	varchar2			号段，指定号段查询时不为空
	--	strStartTime	in	varchar2			开始时间
	--	strEndTime		in	varchar2			结束时间
	--	iPage			in	number				当前页码
	--	iPageSize		in	number				每页显示的记录条数
	--	results			out	cursorType			数量
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure IMSICycBatchList(strDataSeg in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
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

		temp_sql_recodes := 'select count(*) as records from DM_IMSI_CYC_BATCH where SEQ_NUM is not null';
		temp_sql_list := 'select * from ( ';
		temp_sql_list := temp_sql_list || 'select a.SEQ_NUM,a.REGION_CODE,a.HLR_CODE,a.ATTR_ID,a.DATA_SEG,a.IMSI_COUNT,to_char(a.IMPORT_TIME, ''yyyy-mm-dd hh24:mi:ss'') as IMPORT_TIME,a.IMPORT_TYPE, a.CYC_ERRCOUNT,a.CYC_SUCCCOUNT, a.NOTES,a.CYC_BATCH_STAUTS,b.REGION_NAME, ROWNUM NUM ';
		temp_sql_list := temp_sql_list || 'from (select * from DM_IMSI_CYC_BATCH where SEQ_NUM is not null ';

		if strDataSeg is not null then
			temp_sql_recodes := temp_sql_recodes || ' and DATA_SEG=''' || strDataSeg || '''' ;
			temp_sql_list := temp_sql_list || ' and DATA_SEG=''' || strDataSeg || '''' ;
		end if;

		if strStartTime is not null and strEndTime is not null then
			temp_sql_recodes := temp_sql_recodes || ' and IMPORT_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
			temp_sql_list := temp_sql_list || ' and IMPORT_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
		end if;

		temp_sql_list := temp_sql_list || ' order by SEQ_NUM) a, DM_REGION b where a.REGION_CODE=b.REGION_CODE ';
		temp_sql_list := temp_sql_list || ') where  NUM between ' || temp_minPage || ' and ' || temp_maxPage;

		-- 执行
		execute immediate temp_sql_recodes into records;
		open results for temp_sql_list;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end IMSICycBatchList;

	-----------------------------------------------------------------------------------------
	-- IMSICycBatchList
	-- 功能：IMSI导入批次列表查询
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycBatchList(?,?,?,?,?,?) }
	-- 参数：
	--	strRegionCode	in	varchar2			地市代码
	--	strStartTime	in	varchar2			开始时间
	--	strEndTime		in	varchar2			结束时间
	--	iPage			in	number				当前页码
	--	iPageSize		in	number				每页显示的记录条数
	--	results			out	cursorType			数量
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure XHIMSICycBatchList(strRegionCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
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

		temp_sql_recodes := 'select count(*) as records from DM_IMSI_CYC_XH_BATCH where SEQ_NUM is not null';
		temp_sql_list := 'select * from ( ';
		temp_sql_list := temp_sql_list || 'select a.SEQ_NUM,a.REGION_CODE,a.HLR_CODE,a.ATTR_ID,a.DATA_SEG,nvl(c.IMSI_COUNT, 0) as IMSI_COUNT,to_char(a.IMPORT_TIME, ''yyyy-mm-dd hh24:mi:ss'') as IMPORT_TIME,a.IMPORT_TYPE, a.CYC_ERRCOUNT,a.CYC_SUCCCOUNT, a.NOTES,a.CYC_BATCH_STAUTS,b.REGION_NAME, ROWNUM NUM ';
		temp_sql_list := temp_sql_list || 'from (select * from DM_IMSI_CYC_XH_BATCH where SEQ_NUM is not null ';

		temp_sql_recodes := temp_sql_recodes || ' and region_code = ''' || strRegionCode || ''' ';
		temp_sql_list := temp_sql_list || ' and region_code = ''' || strRegionCode || ''' ';

		if strStartTime is not null and strEndTime is not null then
			temp_sql_recodes := temp_sql_recodes || ' and IMPORT_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
			temp_sql_list := temp_sql_list || ' and IMPORT_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
		end if;

		temp_sql_list := temp_sql_list || ' order by SEQ_NUM) a, DM_REGION b left join (select count(1) as imsi_count, region_code from DM_IMSI_CYC_XH where status = '''|| DM_PKG_CONST.IMSI_CYC_STATUS_UNUSED ||''' and region_code = ''' || strRegionCode || ''' group by region_code) c on b.region_code = c.region_code where a.REGION_CODE=b.REGION_CODE ';
		temp_sql_list := temp_sql_list || ') where  NUM between ' || temp_minPage || ' and ' || temp_maxPage;

		-- 执行
		execute immediate temp_sql_recodes into records;
		open results for temp_sql_list;

		retid	:= 0;
		retmsg	:= '成功。';

		-- 系统异常处理
		exception
			when others then
				retid	:= sqlcode;
				retmsg	:= sqlerrm;
	end XHIMSICycBatchList;

	-----------------------------------------------------------------------------------------
	-- IMSICycBackupDataDel
	-- 功能：IMSI导入批次列表查询
	-- 调用：{ DM_PKG_IMSI_CYC.IMSICycBackupDataDel(?,?) }
	-- 参数：
	--	retid			out	number				成功/失败信息ID
	--	retmsg			out	varchar2			成功/失败信息描述
	-- 返回值:
	--	retid			:= 0;
	--	retmsg			:= '成功。';
	-----------------------------------------------------------------------------------------
	procedure IMSICycBackupDataDel
	is
		temp_flag number(11) := -1;
	begin
		select count(*) into temp_flag from DM_IMSI_CYC_BACKUP where generate_time<sysdate-15;
		insert into DM_IMSI_DEL (IMSI_COUNT,NOTES) values(temp_flag,'');
		delete from DM_IMSI_CYC_BACKUP where generate_time<sysdate-15;
		commit;
	end IMSICycBackupDataDel;

  -----------------------------------------------------------------------------------------
  -- 名称: IMSICycBacthInfo
  -- 功能: 获取IMSI回收批次信息
  -- 调用: { call DM_PKG_IMSI_CYC.IMSICycBacthInfo(?,?,?,?) }
  -- 参数:
  --  strSeqNum   in  varchar2      IMSI回收批次
  --  results     out cursorType      游标类型结果集
  --  retid       out number        成功/失败信息ID
  --  retmsg      out varchar2      成功/失败信息描述
  -- 返回值:
  --  records     := 查询出的记录条数
  --  results     := 游标类型结果集
  --  retid       := 8107;
  --  retmsg      := '白卡批次[strBatchNum]信息不存在。';
  --  retid       := 0;
  --  retmsg      := '成功!';
  -----------------------------------------------------------------------------------------
  procedure IMSICycBacthInfo(strSeqNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_IMSI_CYC_BATCH a where a.SEQ_NUM=strSeqNum ;
    if temp_flag < 1 then
      retid := 8096;
      retmsg  := 'IMSI回收批次[' || strSeqNum || ']信息不存在。';
      return;
    end if;

    open results for
      SELECT a.SEQ_NUM,a.REGION_CODE,a.HLR_CODE,a.ATTR_ID,a.DATA_SEG,a.CYC_SUCCCOUNT,a.CYC_ERRCOUNT,a.IMSI_COUNT,
      to_char(a.IMPORT_TIME, 'yyyy-mm-dd hh24:mi:ss') as IMPORT_TIME,A.IMPORT_TYPE,a.NOTES,a.CYC_BATCH_STAUTS,b.REGION_NAME
      from DM_IMSI_CYC_BATCH a, DM_REGION b where a.SEQ_NUM=strSeqNum and a.REGION_CODE=b.REGION_CODE;

    retid := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end IMSICycBacthInfo;

 -----------------------------------------------------------------------------------------
  -- 功能：根据序列号查询回收imsi错误的数据
  -- strSeqNum      in       varchar2  序列
  -- regionName     in       varchar2  地市名
  -- dataSeg        in       varchar2  号段
  -- imsiCount      in       NUMBER    imsi 数量
  -- 返回值
  -- results
  -- retid        = 0
  -- retmsg       = '成功'
  -----------------------------------------------------------------------------------------
  PROCEDURE IMSIExportCycErrInfos(strSeqNum IN VARCHAR2,results OUT cursorType, retid OUT NUMBER, retmsg OUT varchar2)
  is
    temp_str     VARCHAR2(1024):='';
    imsi_count1  NUMBER        :=-1;
    imsi_count2  NUMBER        :=-1;
  BEGIN

    SELECT  count(*) INTO imsi_count1 FROM  dm_imsi_cyc_batch WHERE seq_num=strSeqNum;
    IF imsi_count1 = 0 THEN
      retid :=8097;
      retmsg := '导出的批次：'||strSeqNum||'在批次表不存在！！！';
      RETURN;
    END IF;
    --比较数量
    temp_str := 'SELECT COUNT(1) FROM DM_SYNC_IMSI_CYC_ERR_REOCRD WHERE seq_num=:1 ';
    EXECUTE IMMEDIATE temp_str INTO imsi_count2 USING strSeqNum;
    IF imsi_count2 = 0 THEN
      retid :=  8098;
      retmsg := '导出的批次：'||strSeqNum||'下没有错误数据！！！';
      RETURN;
    END IF;

   SELECT cyc_errcount INTO imsi_count1 FROM  dm_imsi_cyc_batch WHERE seq_num=strSeqNum;
   IF imsi_count1 <> imsi_count2 THEN
       retid :=  8099;
       retmsg := '导出的批次：'||strSeqNum||'下错误数据数量不匹配！！！';
      RETURN;
     END IF ;
    --获取信息
    --返回 cursor 游标 生成 result 结果集
    temp_str:= 'SELECT SEQ_NUM ,REGION_CODE,NET_SEG,DATA_SEG,IMSI,STATUS,CYC_TIME,ERR_MESSAGE,OPRATE_TIME
                FROM DM_SYNC_IMSI_CYC_ERR_REOCRD   WHERE seq_num='''||strSeqNum||'''';
    OPEN results FOR temp_str;

    --'错误信息已导出'
    UPDATE DM_IMSI_CYC_BATCH SET cyc_batch_stauts=DM_PKG_CONST.BATCH_CYC_IMSI_STATE_3 ,notes='已导出错误数据' WHERE seq_num=strSeqNum;

    COMMIT;

    retid :=  0;
    retmsg := '导出成功';

    EXCEPTION
      WHEN OTHERS THEN
        retid :=  SQLCODE;
        retmsg := SQLERRM;

  end IMSIExportCycErrInfos;

end DM_PKG_IMSI_CYC;
//

delimiter ;//