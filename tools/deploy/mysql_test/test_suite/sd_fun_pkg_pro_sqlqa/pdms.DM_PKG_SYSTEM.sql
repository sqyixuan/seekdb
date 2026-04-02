delimiter //;
create or replace package pdms.DM_PKG_SYSTEM is

    type cursorType is ref cursor;

    procedure UserList(results out cursorType, retid out number, retmsg out varchar2);

    procedure UserInfo(strUserName in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure UserAdd(strUserName in varchar2, strPassword in varchar2, retid out number, retmsg out varchar2);

    procedure UserModify(strUsername in varchar2, strPassword in varchar2, strOldPassword in varchar2, retid out number, retmsg out varchar2);

    procedure UserFailedTimes(iUserID in number, iLoginFailedTimes in number, retid out number, retmsg out varchar2);

    procedure UserDel(iUserID in number, retid out number, retmsg out varchar2);

    procedure LogList(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

    procedure LogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure LogInfo(iLogID in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure LogAdd(strUserName in varchar2, iLogType in varchar2, strLogIP in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2);

    procedure LogDel(strLogIDs in varchar2, retid out number, retmsg out varchar2);

    procedure LogClear(retid out number, retmsg out varchar2);

    procedure MenuList(retParent out cursorType, retSub out cursorType, retid out number, retmsg out varchar2);

    procedure CheckCodeAdd(strRequestIp in varchar2, strCheckCode in varchar2, retid out number, retmsg out varchar2);

    procedure CheckCodeInfo(strRequestIp in varchar2, results out cursorType, retid out number, retmsg out varchar2);

    procedure CheckCodeDel(strRequestIp in varchar2, retid out number, retmsg out varchar2);

    procedure SaveIDKey(strIDKey in varchar2, retid out number, retmsg out varchar2);

    procedure VerifyLogin(strIDKey in varchar2, retid out number, retmsg out varchar2);

end DM_PKG_SYSTEM;
//

create or replace package body pdms.DM_PKG_SYSTEM is

    procedure UserList(results out cursorType, retid out number, retmsg out varchar2)
    is
    begin
        open results for
            select * from (
                select USER_ID, USER_NAME, PASS_WORD,to_char(REG_DATE, 'yyyy-mm-dd hh24:mi:ss') as REG_DATE, ROWNUM as NUM
                from DM_USER where USER_NAME <> 'admin'
                order by USER_ID desc
            );

        retid    := 0;
        retmsg    := '成功!';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end UserList;


    procedure UserInfo(strUserName in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from DM_USER where USER_NAME=strUserName;
        if temp_flag < 1 then
            retid    := 1001;
            retmsg    := '系统找不到指定用户[' || strUserName || ']信息。';
            return;
        end if;

        open results for
            select USER_ID, USER_NAME, PASS_WORD,to_char(REG_DATE, 'yyyy-mm-dd hh24:mi:ss') as REG_DATE, LOGIN_FAILED_TIMES, ALIAS
            from DM_USER where USER_NAME=strUserName;

        retid    := 0;
        retmsg    := '成功!';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end UserInfo;

    procedure UserAdd(strUserName in varchar2, strPassword in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from DM_USER where USER_NAME=strUserName;
        if temp_flag > 0 then
            retid    := 1002;
            retmsg    := '该用户[' || strUserName || ']已经存在。';
            return;
        end if;

        insert into DM_USER (user_id, user_name, pass_word,reg_date,login_failed_times) values(SEQ_DM_USER.nextval, strUserName, strPassword, sysdate, 0);
        commit;

        retid    := 0;
        retmsg    := '成功！';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserAdd;


    procedure UserModify(strUsername in varchar2, strPassword in varchar2, strOldPassword in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
        temp_old_pw varchar2(32) := null;
    begin
    select count(*) into temp_flag from DM_USER where USER_NAME=strUsername;
    if temp_flag < 1 then
      retid    := 1004;
      retmsg    := '系统找不到指定用户[' || strUsername || ']信息。';
      return;
    end if;

    select PASS_WORD into temp_old_pw from DM_USER where USER_NAME=strUsername;
    if strOldPassword <> temp_old_pw then
      retid    := 1005;
      retmsg    := '原密码错误！';
      return;
    end if;

        update DM_USER set PASS_WORD=strPassword where USER_NAME=strUsername;
        commit;

        retid    := 0;
        retmsg    := '修改成功';


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
        select count(*) into temp_flag from DM_USER where USER_ID=iUserID;
        if temp_flag < 1 then
            retid    := 1008;
            retmsg    := '系统找不到ID为[' || iUserID || ']的用户信息。';
            return;
        else
            update DM_USER set LOGIN_FAILED_TIMES=iLoginFailedTimes where USER_ID=iUserID;
            commit;
        end if;
        retid    := 0;
        retmsg    := '成功';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end UserFailedTimes;


    procedure UserDel(iUserID in number, retid out number, retmsg out varchar2)
    is
    begin
        delete DM_USER where USER_ID=iUserID;
        commit;

        retid    := 0;
        retmsg    := '成功';

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
                select count(*) into records from DM_LOG;
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select * from DM_LOG order by LOG_TIME desc )
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from DM_LOG
                where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select *
                            from DM_LOG
                            where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                select count(*) into records from DM_LOG where LOG_TYPE=iLogType;
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from ( select *
                            from DM_LOG
                            where LOG_TYPE = iLogType
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            else
                select count(*) into records from DM_LOG
                where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
                open results for
                    select * from (
                        select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME, ROWNUM NUM
                        from (select *
                            from DM_LOG
                            where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                            order by LOG_TIME desc)
                    ) where NUM between temp_minPage and temp_maxPage;
            end if;
        end if;

        retid    := 0;
        retmsg    := '成功。';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end LogList;


    procedure LogExport(iLogType in number, strStartTime in varchar2, strEndTime in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
        temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
    begin
        if iLogType is null or iLogType <= 0 then
            if strStartTime is null and strEndTime is null then
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from DM_LOG
                    order by LOG_ID desc;
            else
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from DM_LOG
                    where LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by LOG_ID desc;
            end if;
        else
            if strStartTime is null and strEndTime is null then
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from DM_LOG
                    where LOG_TYPE = iLogType
                    order by LOG_ID desc;
            else
                open results for
                    select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME
                    from DM_LOG
                    where LOG_TYPE=iLogType and LOG_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'))
                    order by LOG_ID desc;
            end if;
        end if;

        retid    := 0;
        retmsg    := '成功。';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end LogExport;

    procedure LogInfo(iLogID in varchar2, results out cursorType, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from DM_LOG where LOG_ID=iLogID;
        if temp_flag < 1 then
            retid    := 1006;
            retmsg    := '指定的日志信息不存。';
            return;
        end if;

        open results for
            select LOG_ID,USER_NAME,LOG_TYPE,LOG_IP,LOG_CONTENT,to_char(LOG_TIME, 'yyyy-mm-dd hh24:mi:ss') as LOG_TIME from DM_LOG where LOG_ID=iLogID;

        retid    := 0;
        retmsg    := '成功。';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end LogInfo;


    procedure LogAdd(strUserName in varchar2, iLogType in varchar2, strLogIP in varchar2, strLogContent in varchar2, retid out number, retmsg out varchar2)
    is
    begin
        insert into DM_LOG values (SEQ_DM_LOG.nextval, strUserName, iLogType, strLogIP, strLogContent, sysdate);
        commit;

        retid    := 0;
        retmsg    := '成功。';

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
            DM_PKG_UTILS.tokenizer(1, ',', temp_src, temp_str, temp_next);
            temp_src := temp_next;
            delete from DM_LOG where LOG_ID=to_number(temp_str);
        end loop;
        commit;

        retid    := 0;
        retmsg    := '成功。';


        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end LogDel;

    procedure LogClear(retid out number, retmsg out varchar2)
    is
    begin
        delete from DM_LOG;
        commit;

        retid    := 0;
        retmsg    := '成功。';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end LogClear;


    procedure MenuList(retParent out cursorType, retSub out cursorType, retid out number, retmsg out varchar2)
    is
    begin
        open retParent for
            select t.attr_id, t.attr_name from DM_HLR_LINK t where t.attr_level=1;

        open retSub for
            select t.attr_id, t.attr_name, t.parent_id from DM_HLR_LINK t where t.attr_level=2 order by t.attr_name asc;

        retid    := 0;
        retmsg    := '成功。';


        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end MenuList;


    procedure CheckCodeAdd(strRequestIp in varchar2, strCheckCode in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
        select count(*) into temp_flag from DM_CHECK_CODE where REQUEST_IP=strRequestIp;
        if temp_flag < 1 then
            insert into DM_CHECK_CODE values (strRequestIp, strCheckCode);
        else
            update DM_CHECK_CODE set CHECK_CODE=strCheckCode where REQUEST_IP=strRequestIp;
        end if;
        commit;

        retid    := 0;
        retmsg    := '成功';

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
        select count(*) into temp_flag from DM_CHECK_CODE where REQUEST_IP=strRequestIp;
        if temp_flag < 1 then
            retid    := 1007;
            retmsg    := '系统找不到IP为[' || strRequestIp || ']的校验码信息。';
            return;
        end if;

        open results for
            select REQUEST_IP,CHECK_CODE from DM_CHECK_CODE where REQUEST_IP=strRequestIp;


        retid    := 0;
        retmsg    := '成功';


        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end CheckCodeInfo;


    procedure CheckCodeDel(strRequestIp in varchar2, retid out number, retmsg out varchar2)
    is
    begin
        delete from DM_CHECK_CODE where REQUEST_IP=strRequestIp;
        commit;

        retid    := 0;
        retmsg    := '成功';

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
                rollback;
    end CheckCodeDel;

    procedure SaveIDKey(strIDKey in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
    begin
            if strIDKey is null then
                retid    := 0;
                retmsg    := '登录身份Key错误。';
            end if;

            select count(*) into temp_flag from DM_LOGIN_IDKEY;
            if temp_flag < 1 then
                insert into DM_LOGIN_IDKEY values(strIDKey, 0, '');
            else
                update DM_LOGIN_IDKEY set ID_KEY = strIDKey;
            end if;

            commit;

            retid    := 0;
            retmsg    := '成功。';

        exception
            when others then
                retid    := sqlcode;
                retmsg := sqlerrm;
    end SaveIDKey;


    procedure VerifyLogin(strIDKey in varchar2, retid out number, retmsg out varchar2)
    is
        temp_flag number := -1;
        temp_id_key varchar2(8) := null;
    begin
        select count(*) into temp_flag from DM_LOGIN_IDKEY ;
        if temp_flag < 1 then
            retid := -1;
            retmsg := '用户未登录，请登录。';
        end if;

        select ID_KEY into temp_id_key from DM_LOGIN_IDKEY ;

        if temp_id_key = strIDKey then
            retid    := 0;
            retmsg    := '成功。';
        else
            retid := -1;
            retmsg := '本系统已在其他地方登录，将退出或重新登录。';
        end if;

        exception
            when others then
                retid    := sqlcode;
                retmsg    := sqlerrm;
    end VerifyLogin;

end DM_PKG_SYSTEM;
//

delimiter ;//