
  CREATE OR REPLACE PACKAGE "OPS_BASIC" is
   
    type cursorType is ref cursor;
    
   
    procedure RoleList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);
        
    
    procedure UserList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);
        

    procedure UserInfo(strUserName in varchar2, results out cursorType, retid out number, retmsg out varchar2);
    

    procedure UserAdd(strUserName in varchar2, strNickName in varchar2, strPassword in varchar2, iRoleID in number, retid out number, retmsg out varchar2);
    

    procedure UserModify(iUserID in number, strOldPassword in varchar2, strPassword in varchar2, strModObject in varchar2, strOtherUserName in varchar2, retid out number, retmsg out varchar2);    
    

    procedure UserFailedTimes(iUserID in number, iLoginFailedTimes in number, retid out number, retmsg out varchar2);    


    procedure UserDel(iUserID in number, retid out number, retmsg out varchar2);
  
    procedure LogList(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);
    
   
    procedure LogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure LogInfo(iLogID in varchar2, results out cursorType, retid out number, retmsg out varchar2);
    
    procedure LogAdd(strUserName in varchar2, iLogType in varchar2, strLogIP in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2);

    procedure LogDel(strLogIDs in varchar2, retid out number, retmsg out varchar2);

    procedure LogClear(retid out number, retmsg out varchar2);

    procedure IFLogList(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

    procedure IFLogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure IFLogAdd(strSeqNo in varchar2, iLogType in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2);

    procedure IFLogDel(strLogIDs in varchar2, retid out number, retmsg out varchar2);

    procedure IFLogClear(retid out number, retmsg out varchar2);

    procedure CheckCodeAdd(strRequestIp in varchar2, strCheckCode in varchar2, retid out number, retmsg out varchar2);

    procedure CheckCodeInfo(strRequestIp in varchar2, results out cursorType, retid out number, retmsg out varchar2);
    
    procedure CheckCodeDel(strRequestIp in varchar2, retid out number, retmsg out varchar2);
    
end OPS_BASIC;

//

  CREATE OR REPLACE PACKAGE BODY "OPS_BASIC" is

    procedure RoleList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_minPage number(11) := -1;
        temp_maxPage number(11) := -1;
    begin
        temp_minPage := ((iPage - 1) * iPageSize) + 1;
        temp_maxPage := iPage * iPageSize;
        
        select COUNT(*) into records from OPS_ROLE;
        open results for
            select * from (
                select ROLE_ID,ROLE_NAME,NOTES,ROWNUM as NUM from OPS_ROLE order by ROLE_ID desc
            ) where NUM between temp_minPage and temp_maxPage;
        
        retid    := 0;
        retmsg    := '¿¿';
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end RoleList;
    
  
    procedure UserList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_minPage number(11) := -1;
        temp_maxPage number(11) := -1;
    begin
        temp_minPage := ((iPage - 1) * iPageSize) + 1;
        temp_maxPage := iPage * iPageSize;
        
        select COUNT(*) into records from OPS_USER a, OPS_ROLE b where a.ROLE_ID=b.ROLE_ID and a.USER_NAME <> 'admin';
        open results for
            select * from (
                select a.USER_ID,a.USER_NAME,a.NICK_NAME,a.PASS_WORD,to_char(a.REG_DATE, 'yyyy-mm-dd hh24:mi:ss') as REG_DATE,a.ROLE_ID,b.ROLE_NAME,ROWNUM as NUM
                from OPS_USER a, OPS_ROLE b where a.ROLE_ID=b.ROLE_ID and a.USER_NAME <> 'admin'
                order by USER_ID desc
            ) where NUM between temp_minPage and temp_maxPage;
        
        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end UserList;
    
   
    procedure UserInfo(strUserName in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from OPS_USER a,OPS_ROLE b where a.ROLE_ID=b.ROLE_ID and USER_NAME=strUserName;
        if temp_flag < 1 then
            retid    := 1006;
            retmsg    := '1' || strUserName || '2';
            return;
        end if;
        
        open results for
            select a.USER_ID,a.USER_NAME,a.NICK_NAME,a.PASS_WORD,to_char(a.REG_DATE, 'yyyy-mm-dd hh24:mi:ss') as REG_DATE,a.ROLE_ID,b.ROLE_NAME,a.LOGIN_FAILED_TIMES
            from OPS_USER a, OPS_ROLE b where a.ROLE_ID=b.ROLE_ID and a.USER_NAME=strUserName;
        
        retid    := 0;
        retmsg    := '33';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end UserInfo;
    
    
    procedure UserAdd(strUserName in varchar2, strNickName in varchar2, strPassword in varchar2, iRoleID in number, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from OPS_USER where USER_NAME=strUserName;
        if temp_flag > 0 then
            retid    := 1007;
            retmsg    := strUserName || 'ee';
            return;
        end if;

        insert into OPS_USER values(OPS_SEQ_USER.nextval, strUserName, strPassword, sysdate, iRoleID, 0, strNickName);
        commit;

        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserAdd;
    
  
    procedure UserModify(iUserID in number, strOldPassword in varchar2, strPassword in varchar2, strModObject in varchar2, strOtherUserName in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
        temp_old_pw varchar2(32) := null;
        temp_user_id number := -1;
    begin
        select count(*) into temp_flag from OPS_USER where USER_ID=iUserID;
        if temp_flag < 1 then
            retid    := 1008;
            retmsg    := iUserID || 'dd';
            return;
        end if;
        
        if strModObject = 'Other' then 
            select count(*) into temp_flag from OPS_USER where USER_NAME=strOtherUserName;
            if temp_flag < 1 then
                retid    := 1009;
                retmsg    := strOtherUserName || ']¿';
                return;
            end if;
            
            select USER_ID into temp_user_id from OPS_USER where USER_NAME=strOtherUserName;
        elsif strModObject = 'Self' then         
            select PASS_WORD into temp_old_pw from OPS_USER where USER_ID=iUserID;
            if strOldPassword <> temp_old_pw then
                retid    := 1010;
                retmsg    := 'cc';
                return;
            end if;
            
            temp_user_id := iUserID;
        end if;

        update OPS_USER set PASS_WORD=strPassword where USER_ID=temp_user_id;
        commit;

        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserModify;
    
  
    procedure UserFailedTimes(iUserID in number, iLoginFailedTimes in number, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from OPS_USER where USER_ID=iUserID;
        if temp_flag < 1 then
            retid    := 1015;
            retmsg    := iUserID || 'b';
            return;
        else
            update OPS_USER set LOGIN_FAILED_TIMES=iLoginFailedTimes where USER_ID=iUserID;
            commit;
        end if;
        retid    := 0;
        retmsg    := '¿¿';

      
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserFailedTimes;

    
    procedure UserDel(iUserID in number, retid out number, retmsg out varchar2)
    is
    begin
        delete OPS_USER where USER_ID=iUserID;
        commit;

        retid    := 0;
        retmsg    := '¿¿';
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserDel;
    
    
    procedure LogList(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)        
    is
        temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
        temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
        temp_minPage number(11) := -1;
        temp_maxPage number(11) := -1;
    begin        
        temp_minPage := ((iPage - 1) * iPageSize) + 1;
        temp_maxPage := iPage * iPageSize;
        
        if iLogType is null or iLogType <= 0 then
            if strStartTime is null and strEndTime is null then
                select count(*) into records from OPS_LOG;
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select * from OPS_LOG order by LOG_TIME desc )
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from OPS_LOG
                where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select *
                            from OPS_LOG
                            where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                select count(*) into records from OPS_LOG where LOG_TYPE=iLogType;
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from ( select * 
                            from OPS_LOG
                            where LOG_TYPE = iLogType
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from OPS_LOG
                where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select * 
                            from OPS_LOG
                            where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        end if;

        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;                
    end LogList;
    
    
    procedure LogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
    begin
        if iLogType is null or iLogType <= 0 then
            if strStartTime is null and strEndTime is null then
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_LOG
                    order by LOG_ID desc;
            else
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_LOG
                    where LOG_TIME between (to_date(strStartTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(strEndTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by LOG_ID desc;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_LOG
                    where LOG_TYPE = iLogType
                    order by LOG_ID desc;
            else
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_LOG
                    where LOG_TYPE=iLogType and LOG_TIME between (to_date(strStartTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(strEndTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by LOG_ID desc;
            end if;
        end if;
        
        retid    := 0;
        retmsg    := '¿¿';
        
      
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;                
    end LogExport;
    
   
    procedure LogInfo(iLogID in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from OPS_LOG where LOG_ID=iLogID;
        if temp_flag < 1 then
            retid    := 1011;
            retmsg    := iLogID || 34;
            return;
        end if;
        
        open results for
            select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME from OPS_LOG where LOG_ID=iLogID;
        
        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end LogInfo;
    
   
    procedure LogAdd(strUserName in varchar2, iLogType in varchar2, strLogIP in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2)
    is
    begin        
        insert into OPS_LOG values (OPS_SEQ_LOG.nextval, strUserName, iLogType, strLogIP, strLogContent, sysdate);
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end LogAdd;
     
   
    procedure LogDel(strLogIDs in varchar2, retid out number, retmsg out varchar2)
    is
        temp_src varchar2(255) := strLogIDs;
        temp_str varchar2(11) := null;
        temp_next varchar2(255) := ' ';
    begin
        while (temp_next is not null) loop
            OPS_UTILS.tokenizer(1, ',', temp_src, temp_str, temp_next);
            temp_src := temp_next;
            delete from OPS_LOG where LOG_ID=to_number(temp_str);
        end loop;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';
        
     
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end LogDel;    
    
    
    procedure LogClear(retid out number, retmsg out varchar2)
    is
    begin
    
        delete from OPS_LOG;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;                
    end LogClear;
        
    
    procedure IFLogList(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)    
    is
        temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
        temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
        temp_minPage number(11) := -1;
        temp_maxPage number(11) := -1;
    begin        
        temp_minPage := ((iPage - 1) * iPageSize) + 1;
        temp_maxPage := iPage * iPageSize;
        
        if iLogType is null or iLogType <= 0 or iLogType >=5 then
            if strStartTime is null and strEndTime is null then
                select count(*) into records from OPS_IF_LOG;
                open results for
                    select * from (
                        select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select * from OPS_IF_LOG order by LOG_TIME desc )
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from OPS_IF_LOG
                where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select *
                            from OPS_IF_LOG
                            where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                select count(*) into records from OPS_IF_LOG where LOG_TYPE=iLogType;
                open results for
                    select * from (
                        select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from ( select * 
                            from OPS_IF_LOG
                            where LOG_TYPE = iLogType
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from OPS_IF_LOG
                where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select * 
                            from OPS_IF_LOG
                            where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        end if;

        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;                
    end IFLogList;
    
    
    procedure IFLogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
    begin
        if iLogType is null or iLogType <= 0 or iLogType >=5 then
            if strStartTime is null and strEndTime is null then
                open results for
                    select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_IF_LOG
                    order by IF_LOG_ID desc;
            else
                open results for
                    select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_IF_LOG
                    where LOG_TIME between (to_date(strStartTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(strEndTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by IF_LOG_ID desc;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                open results for
                    select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_IF_LOG
                    where LOG_TYPE = iLogType
                    order by IF_LOG_ID desc;
            else
                open results for
                    select IF_LOG_ID,SEQ_NO,LOG_TYPE,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from OPS_IF_LOG
                    where LOG_TYPE=iLogType and LOG_TIME between (to_date(strStartTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(strEndTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by IF_LOG_ID desc;
            end if;
        end if;
        
        retid    := 0;
        retmsg    := '¿¿';
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;                
    end IFLogExport;
    
   
    procedure IFLogAdd(strSeqNo in varchar2, iLogType in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2)
    is
    begin        
        insert into OPS_IF_LOG values (OPS_SEQ_IF_LOG.nextval, strSeqNo, iLogType, strLogContent, sysdate);
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end IFLogAdd;
         
   
    procedure IFLogDel(strLogIDs in varchar2, retid out number, retmsg out varchar2)
    is
        temp_src varchar2(255) := strLogIDs;
        temp_str varchar2(11) := null;
        temp_next varchar2(255) := ' ';
    begin
        while (temp_next is not null) loop
            OPS_UTILS.tokenizer(1, ',', temp_src, temp_str, temp_next);
            temp_src := temp_next;
            delete from OPS_IF_LOG where IF_LOG_ID=to_number(temp_str);
        end loop;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';
        
        
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end IFLogDel;    
    
  
    procedure IFLogClear(retid out number, retmsg out varchar2)
    is
    begin
        delete from OPS_IF_LOG;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';
        
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;                
    end IFLogClear;
    
   
    procedure CheckCodeAdd(strRequestIp in varchar2, strCheckCode in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin 
        select count(*) into temp_flag from OPS_CHECK_CODE where REQUEST_IP=strRequestIp;
        if temp_flag < 1 then
            insert into OPS_CHECK_CODE values (strRequestIp, strCheckCode);
        else
            update OPS_CHECK_CODE set CHECK_CODE=strCheckCode where REQUEST_IP=strRequestIp;
        end if;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end CheckCodeAdd;

   
    procedure CheckCodeInfo(strRequestIp in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin 
        select count(*) into temp_flag from OPS_CHECK_CODE where REQUEST_IP=strRequestIp;
        if temp_flag < 1 then
            retid    := 1012;
            retmsg    := strRequestIp || 'a';
            return;
        end if;
        
        open results for
            select REQUEST_IP,CHECK_CODE from OPS_CHECK_CODE where REQUEST_IP=strRequestIp;
        
        
        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end CheckCodeInfo;
        
   
    procedure CheckCodeDel(strRequestIp in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin 
        delete from OPS_CHECK_CODE where REQUEST_IP=strRequestIp;
        commit;
        
        retid    := 0;
        retmsg    := '¿¿';

       
        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;        
    end CheckCodeDel;
    
end OPS_BASIC;
//