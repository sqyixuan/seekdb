delimiter /;
CREATE OR REPLACE FUNCTION "IM"."PSI_GETSTATDAYPSITYPE" (inoutFlag    IN VARCHAR2,
                                                   busiType     IN VARCHAR2,
                                                   subsBusiType IN VARCHAR2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(32);
BEGIN
  v_return := null;
  if inoutFlag = 'IN' and busiType = 'LDSTORE' then
    v_return := 'LDSTORE_NUM'; --入库数量
  elsif inoutFlag = 'OUT' and busiType = 'WITHDRAW' then
    v_return := 'REFUND_NUM'; --退库数量
  elsif inoutFlag = 'IN' and busiType = 'ASIGIN' then
    v_return := 'ASIGIN_NUM'; --调入数量
  elsif inoutFlag = 'OUT' and busiType = 'ASIGOUT' then
    v_return := 'ASIGOUT_NUM'; --调出数量
  elsif inoutFlag = 'OUT' and busiType = 'SALE' then
    v_return := 'SALE_NUM'; --销售数量
  elsif inoutFlag = 'IN' and busiType = 'UNSALE' then
    v_return := 'SALEBACK_NUM'; --销售回退数量
  else
    v_return := null;
  end if;
  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END psi_getStatDaypsiType;
/

delimiter ;/



delimiter /;
CREATE OR REPLACE FUNCTION "IM"."QRYCHANNELTYPE" (i_orgid IN VARCHAR2,i_region IN VARCHAR2)
RETURN VARCHAR2
IS
       v_orgType VARCHAR2(20);
       v_result  VARCHAR2(8);
       BEGIN
       SELECT CASE
           WHEN A.ORGTYPE IN ('ogkdAgent', 'ogkdDept') THEN
            (CASE
                WHEN (SELECT M.AGENTTYPE FROM AGENT M WHERE M.AGENTID = A.ORGID) IN
                     ( SELECT DICT_ID FROM PSI_DICT_ITEM WHERE GROUP_ID='LOC_FAN_OGKDAGENT') THEN
                 'ogkdAgent'
                ELSE
                 'ogkdDept'
            END)
           ELSE
            NVL((SELECT M.PARTNER_TYPE
                FROM PSI_INFO_PARTNER M
                WHERE M.ORG_id = A.ORGID)
               , A.ORGTYPE)
      END ORG_TYPE INTO v_orgType
      FROM ORGANIZATION A
     WHERE orgid=i_orgid AND REGION=i_region AND ROWNUM = 1;
           IF v_orgType !='ogkdAgent' THEN
             v_result:='SELF';
           ELSE
             v_result:='AGENT';
           END IF;
        RETURN(v_result);
        EXCEPTION
            WHEN OTHERS THEN
            RETURN NULL;
END QRYCHANNELTYPE;
/

delimiter ;/
