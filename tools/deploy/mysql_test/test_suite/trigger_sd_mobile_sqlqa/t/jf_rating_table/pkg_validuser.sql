CREATE SEQUENCE  "SEQ_AUDIT"  MINVALUE 0 MAXVALUE 99999999 INCREMENT BY 1 START WITH 21605340 CACHE 20 ORDER  CYCLE ;

CREATE SEQUENCE  "SEQ_BILLINGCLIENTLOG"  MINVALUE 0 MAXVALUE 1000000000 INCREMENT BY 1 START WITH 174402 CACHE 20 NOORDER  NOCYCLE ;

CREATE SEQUENCE  "SEQ_LOGON"  MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1388537 CACHE 100 ORDER  CYCLE ;

CREATE TABLE "APP_USER" ("USER_CODE" VARCHAR2(16), "USER_NAME" VARCHAR2(60), "PASSWORD" VARCHAR2(256), "STATUS" CHAR(1), "DESCRIPTION" VARCHAR2(300), "PASSWORD_HIS" VARCHAR2(4000), "MODIFY_DATE" DATE, "FAIL_COUNT" NUMBER(4,0) DEFAULT 0, "USERTYPE" NUMBER(1,0) DEFAULT 0, "PWDTYPE" NUMBER(1,0) DEFAULT 0, "REGION" NUMBER(5,0), "SESSIONID" VARCHAR2(30), "LOGONTIME" DATE, "LOGOUTTIME" DATE, "USERSALT_HIS" VARCHAR2(2200), "USERSALT" VARCHAR2(100), "INUSE_STARTTIME" DATE, "INUSE_ENDTIME" DATE) ;

CREATE TABLE "FEATURE_DEF" ("NAME" VARCHAR2(50), "VALUE" VARCHAR2(100), "NOTE" VARCHAR2(600)) ;

CREATE TABLE "AUDIT_LOG" ("LOGNUM" VARCHAR2(24), "OPERATOR" VARCHAR2(16), "LOGTIME" DATE, "TABLENAME" VARCHAR2(64), "MAINTAINTYPE" CHAR(1), "KEYVALUE" VARCHAR2(1024), "COLNAME" VARCHAR2(64), "OLDVALUE" VARCHAR2(4000), "NEWVALUE" VARCHAR2(4000), "AUDIT_STATUS" NUMBER(1,0) DEFAULT 0, "REASON" VARCHAR2(128), "AUDIT_TIME" DATE, "AUDIT_OPERATOR" VARCHAR2(20), "COL_DATATYPE" VARCHAR2(106), "CLIENTINFO" VARCHAR2(1024), "LOGONID" NUMBER(10,0), "LOGONSEQ" VARCHAR2(32), "OPERNUM" VARCHAR2(24))  PARTITION BY RANGE ("LOGTIME")  (PARTITION "P_202006"  VALUES LESS THAN (TO_DATE(' 2020-07-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) , PARTITION "P_MAX"  VALUES LESS THAN (MAXVALUE) ) ;

CREATE TABLE "PARAM_TABLE_DEF" ("TABLE_NAME" VARCHAR2(64), "TABLE_NOTE" VARCHAR2(64), "SERIOUS" NUMBER(1,0), "LASTCHANGETIME" DATE) ;

insert into feature_def(name,value) values('system_mode','wb593756');
insert into app_user(user_code) values('JNLH');
insert into app_user(user_code) values('_SYS_');




delimiter //;
CREATE OR REPLACE PACKAGE "PKG_VALIDUSER" AS
  Logonid       NUMBER(10) := 0;
  Userid        VARCHAR2(100) := '_SYS_';
  Istriggerneed NUMBER := 1;
  Usertype      NUMBER := 0;
  ClientInfo    varchar2(1024) := NULL;
  LogonSeq      VARCHAR2(32) := '0';
  PROCEDURE Setuser(v_User IN VARCHAR2,
                    v_Istriggerneed NUMBER := 1,
                    v_clientinfo in varchar2 := '');
  PROCEDURE Defaultuser(v_Istriggerneed NUMBER := 1);
  PROCEDURE Setusertype(v_Usertype NUMBER := 0);
  PROCEDURE SetLogonid(v_Logonid NUMBER := 0);
  SUBTYPE Auditlogtype IS Audit_Log%ROWTYPE;
  FUNCTION Beginauditlog(a_Auditlog IN OUT Auditlogtype) RETURN NUMBER;
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  VARCHAR2,
                          a_Newvalue  VARCHAR2);
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  DATE,
                          a_Newvalue  DATE);
  PROCEDURE Endauditlog(a_Auditlog IN OUT Auditlogtype);
  FUNCTION  GetUserid(a_userid in varchar2) return varchar2;
  PROCEDURE GetLogonSeq(a_LogonSeq OUT VARCHAR2);
END Pkg_Validuser;
//


CREATE OR REPLACE PACKAGE BODY "PKG_VALIDUSER" AS
    PROCEDURE Setuser(v_User IN VARCHAR2,
                      v_Istriggerneed NUMBER := 1,
                      v_clientinfo in varchar2 := '') AS
        v_Usertype NUMBER:=0;
        v_Logonid  NUMBER:=0;
    BEGIN
        Pkg_Validuser.Userid        := v_User;
        Pkg_Validuser.Istriggerneed := v_Istriggerneed;
        ClientInfo := v_clientinfo;
    select to_char(sysdate, 'yyyymmddhh24miss') ||
         lpad(to_char(seq_logon.nextval), 8, '0')
      into Pkg_Validuser.LogonSeq
      from dual;
        begin
            SELECT Usertype
              INTO v_Usertype
              FROM App_User
             WHERE User_Code = v_User
               AND Rownum = 1;
                Setusertype(v_Usertype);

                SELECT Seq_Billingclientlog.NEXTVAL INTO v_Logonid FROM Dual;
                SetLogonid(v_Logonid);
        exception
           when others then
       null;
        end;
    END;

    PROCEDURE Defaultuser(v_Istriggerneed NUMBER := 1) AS
    BEGIN
    Pkg_Validuser.Userid        := '_SYS_';
    Pkg_Validuser.Istriggerneed := v_Istriggerneed;
    if Pkg_Validuser.LogonSeq = '0' then
        select to_char(sysdate, 'yyyymmddhh24miss') ||
        lpad(to_char(seq_logon.nextval), 8, '0')
        into Pkg_Validuser.LogonSeq
        from dual;
    end if;
    END;

  PROCEDURE Setusertype(v_Usertype NUMBER := 0) AS
    v_SystemMode VARCHAR2(64);
  BEGIN
    select value
      into v_SystemMode
      from feature_def
     where name = 'system_mode';
    if v_SystemMode = 'development' then
      Pkg_Validuser.Usertype := 1;
    else
      Pkg_Validuser.Usertype := v_Usertype;
    end if;
  exception
    when no_data_found then
      Pkg_Validuser.Usertype := v_Usertype;
  END;

    PROCEDURE SetLogonid(v_Logonid NUMBER := 0) AS
    BEGIN
         Pkg_Validuser.Logonid := v_Logonid;
    END;

    function BeginAuditLog(a_AuditLog in out AuditLogType) return number is
       v_SystemMode VARCHAR2(64);
    begin
    begin
       select value into v_SystemMode from feature_def
       where name = 'system_mode';
       if v_SystemMode = 'development' then
         select 'OS_USER'
         into Pkg_Validuser.Userid
         from dual;
       end if;
    exception
       when no_data_found then
       a_AuditLog.Operator := pkg_validuser.userid;
    end;
        if istriggerneed=0 then
            return 0;
        end if;
        if pkg_validuser.userid is null then
            pkg_validuser.userid := '_SYS_';
        end if;


        if ClientInfo is null then
            a_AuditLog.ClientInfo := '127.0.0.1:HOST';
            ClientInfo := a_AuditLog.ClientInfo;
        else
            a_AuditLog.ClientInfo := ClientInfo;
        end if;

        a_AuditLog.Operator := pkg_validuser.userid;

        select to_char(seq_audit.nextval) into a_AuditLog.LogNum from dual;
        a_AuditLog.LogNum:=a_AuditLog.Operator||lpad(a_AuditLog.LogNum,8,'0');
        a_AuditLog.LogTime:=sysdate;

        if a_AuditLog.MaintainType = 'I' then
           a_AuditLog.KeyValue := '';
        end if;
        return 1;
    end;

    procedure WriteAuditLog(a_AuditLog in AuditLogType,
                            a_fieldname varchar2,
                            a_oldvalue date,
                            a_newvalue date)is
    begin
        WriteAuditLog(a_AuditLog, a_fieldname,
                      to_char(a_oldvalue, 'yyyy-mm-dd hh24:mi:ss'),
                      to_char(a_newvalue, 'yyyy-mm-dd hh24:mi:ss'));
    end;

    procedure WriteAuditLog(a_AuditLog in AuditLogType,
                            a_fieldname varchar2,
                            a_oldvalue varchar2,
                            a_newvalue varchar2)is
    begin
      if (a_newvalue is not null and a_oldvalue is null or
       a_newvalue is null and a_oldvalue is not null) or
       (a_newvalue != a_oldvalue) then
      if Pkg_Validuser.LogonSeq = '0' then
        select to_char(sysdate, 'yyyymmddhh24miss') ||
               lpad(to_char(seq_logon.nextval), 8, '0')
          into Pkg_Validuser.LogonSeq
          from dual;
      end if;

      if pkg_validuser.Logonid = 0 then
       SELECT Seq_Billingclientlog.NEXTVAL INTO pkg_validuser.Logonid FROM Dual;
      end if;

      insert into audit_log
        (lognum,
         operator,
         logtime,
         tablename,
         maintaintype,
         keyvalue,
         colname,
         oldvalue,
         newvalue,
         clientinfo,
         logonid,
         LogonSeq,
         OPERNUM)
      values
        (a_AuditLog.LogNum,
         a_AuditLog.Operator,
         a_AuditLog.LogTime,
         a_AuditLog.TableName,
         a_AuditLog.MaintainType,
         a_AuditLog.KeyValue,
         a_fieldname,
         a_oldvalue,
         a_newvalue,
         pkg_validuser.clientinfo,
         pkg_validuser.Logonid,
         pkg_validuser.LogonSeq,
         a_AuditLog.OPERNUM);
        end if;
    end;

    procedure EndAuditLog(a_AuditLog in out AuditLogType) is
    begin
    update param_table_def
       set lastchangetime = sysdate
     where table_name = a_AuditLog.TableName;
    end;

  procedure GetLogonSeq(a_LogonSeq OUT VARCHAR2) is
    begin
    a_LogonSeq := Pkg_Validuser.LogonSeq;
    end;

    function GetUserid(a_userid in varchar2) return varchar2 is
      v_userid varchar2(20);
    begin
      v_userid := Pkg_Validuser.Userid;
      return v_userid;
    end;
END Pkg_Validuser;
//
delimiter ;//
