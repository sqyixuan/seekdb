delimiter //;
CREATE OR REPLACE PROCEDURE IM.AP_SCM_XS_CREATEFILE

IS
  V_FileName         VARCHAR2(50);
  V_FileInsideNO     NUMBER(20);
  V_CurCount         NUMBER(20);
  V_FileNo           NUMBER(20);
  V_ContentHead      VARCHAR2(2000);
  V_VFileNo          VARCHAR2(5);
BEGIN

select
'<?xml version="1.0" encoding="UTF-8"?>'|| CHR(13)||
'<SalesList ProvinceID="'||t.paramvalue||'" BizType="XS">'|| CHR(13) INTO V_ContentHead
 from parameter_value t where t.paramid = 'BusiProvinceCode';

select 'CMDC_XS_M_'||to_char(sysdate,'yyyymmddhh24miss')||'_'||paramvalue||'.' INTO V_FileName from parameter_value t where t.paramid = 'BusiProvinceCode';

V_FileInsideNO := 0;

V_FileNo := 0;

V_CurCount := 0;

if V_FileNo < 10 then
  V_VFileNo := '00'||V_FileNo;
  end IF;

if  V_FileNo >= 10 and V_FileNo < 100 then
  V_VFileNo := '0'||V_FileNo;
  end IF;

if  V_FileNo >= 100 and V_FileNo < 1000 then
  V_VFileNo := V_FileNo;
  end IF;


 for cc in (
  select
chr(9)||chr(9)||'<SalesItem>'|| CHR(13)||
chr(9)||chr(9)||'<EventType>'||BUSITYPE||'</EventType>'|| CHR(13)||
chr(9)||chr(9)||'<SoldIMEI>'||imei||'</SoldIMEI>'|| CHR(13)||
chr(9)||chr(9)||'<ReturnIMEI>'||OUT_IMEI||'</ReturnIMEI>'|| CHR(13)||
chr(9)||chr(9)||'<IsCMDC>'||'</IsCMDC>'|| CHR(13)||
chr(9)||chr(9)||'<EventTime>'||to_char(busidate,'YYYYMMDDHHMMSS')||'</EventTime>'|| CHR(13)||
chr(9)||chr(9)||'<ShopCode>'||decode(t.BUSITYPE,0,t.shopcode,(select m.shopcode from im_cfg_mobileshop m where m.orgid = (select a.org_id from im_inv_mobtel a where (a.inv_id = t.imei or a.inv_id = t.out_imei) and rownum = 1) and rownum = 1))
||'</ShopCode>'|| CHR(13)||
chr(9)||chr(9)||'<SalesKind>'||saleskind||'</SalesKind>'|| CHR(13)||
chr(9)||chr(9)||'<MarketPrice>'||marketprice||'</MarketPrice>'|| CHR(13)||
chr(9)||chr(9)||'<ActualSalesPrice>'||actualsalesprice||'</ActualSalesPrice>'|| CHR(13)||
chr(9)||chr(9)||'<PromotionCode>'||promotioncode||'</PromotionCode>'|| CHR(13)||
chr(9)||chr(9)||'<PromotionType>'||promotiontype||'</PromotionType>'|| CHR(13)||
chr(9)||chr(9)||'<MobileNo>'||mobileno||'</MobileNo>'|| CHR(13)||
chr(9)||chr(9)||'<IsRev1>'||(select b.brand_name from im_res_type i, psi_cfg_resbrand b, im_inv_mobtel m  where (m.inv_id = t.imei or m.inv_id = t.out_imei) and m.res_type_id = i.res_type_id and i.brand_id = b.brand_id and m.region = t.region  and rownum =1)||'</IsRev1>'|| CHR(13)||
chr(9)||chr(9)||'<IsRev2>'||(select c.res_type_name from im_inv_mobtel m,im_res_type c  where (m.inv_id = t.imei or m.inv_id = t.out_imei) and m.res_type_id = c.res_type_id and c.res_table_name = 'IM_INV_MOBTEL'  and m.region = t.region  and rownum =1 )||'</IsRev2>'|| CHR(13)||
chr(9)||chr(9)||'<IsRev3>'||(select m.real_color from im_inv_mobtel m where (m.inv_id = t.imei or m.inv_id = t.out_imei) and m.region = t.region and rownum =1 )||'</IsRev3>'|| CHR(13)||
chr(9)||chr(9)||'<IsRev4>'||''||'</IsRev4>'|| CHR(13)||
chr(9)||chr(9)||'</SalesItem>'||CHR(13) as XMLcontent
 from im_if_bb_reexsell t
 where  t.busidate >=  trunc(sysdate-1) and t.busidate <  trunc(sysdate)
 
) loop

if V_CurCount = 0 then
 insert into SCM_FORFILE(fileName,Fileinsideid,Filetype,xmlcontent) values(V_FileName||V_VFileNo,V_FileInsideNO,'XS',V_ContentHead);
 V_FileInsideNO := V_FileInsideNO +1;
 end if;

 V_CurCount := V_CurCount + 1;

 insert into SCM_FORFILE(fileName,Fileinsideid,Filetype,xmlcontent) values(V_FileName||V_VFileNo,V_FileInsideNO,'XS',cc.XMLcontent);
 V_FileInsideNO := V_FileInsideNO +1;

if V_CurCount >= 4000 then
  insert into SCM_FORFILE(fileName,Fileinsideid,Filetype,xmlcontent) values(V_FileName||V_VFileNo,V_FileInsideNO,'XS',chr(9)||chr(9)|| '</SalesList>'||CHR(13));

  V_FileInsideNO := 1;
  V_CurCount := 0;

  V_FileNo := V_FileNo + 1;
  if V_FileNo < 10 then
  V_VFileNo := '00'||V_FileNo;
  end IF;

  if  V_FileNo >= 10 and V_FileNo < 100 then
  V_VFileNo := '0'||V_FileNo;
  end IF;

  if  V_FileNo >= 100 and V_FileNo < 1000 then
  V_VFileNo := V_FileNo;
  end IF;

  commit;

  end if;

END LOOP;

if V_CurCount >= 1 then
  insert into SCM_FORFILE(fileName,Fileinsideid,Filetype,xmlcontent) values(V_FileName||V_VFileNo,V_FileInsideNO,'XS',chr(9)||chr(9)|| '</SalesList>'||CHR(13));
  commit;
 end if;


END AP_SCM_XS_CREATEFILE;
//

CREATE OR REPLACE PROCEDURE IM.AP_DAILY_MODI_RECORD(V_ENTITY_ID   VARCHAR2,
                                                 V_ORDER_ID    VARCHAR2,
                                                 V_HW_ID       VARCHAR2,
                                                 V_REC_CONTENT VARCHAR2) AS
  V_OID NUMBER(14);
BEGIN
  SELECT DAILY_MODI_RECORD_OID.NEXTVAL INTO V_OID FROM DUAL;
  INSERT INTO DAILY_MODI_RECORD
    (OID, ENTITY_ID, ORDER_ID, HW_ID, REC_CONTENT)
  VALUES
    (V_OID, V_ENTITY_ID, V_ORDER_ID, V_HW_ID, V_REC_CONTENT);
END AP_DAILY_MODI_RECORD;
//

CREATE OR REPLACE PROCEDURE IM.LOC_FUN_SUPPLIER_APPLY IS
BEGIN
  --删除排行表中当前数据
  DELETE FROM PSI_INFO_MOB_RANKINGLIST T WHERE T.RANKING_TYPE = 'FANSUPPLIERAPPLY';
  ---统计最近90日提货数据
  FOR V_RECORD IN (SELECT *
  FROM (SELECT SUM(B.APPLY_NUM) AMT, B.SUPPLIER_ID
          FROM PSI_FLOW_APPLY_BATCH B
         WHERE SUPPLIER_ID IS NOT NULL
           AND B.CREATE_DATE > SYSDATE - 90
         GROUP BY B.SUPPLIER_ID
         ORDER BY SUM(B.APPLY_NUM) DESC)
 WHERE ROWNUM <= 30
) LOOP
    INSERT INTO PSI_INFO_MOB_RANKINGLIST
      (RANKING_TYPE,
       RES_TYPE_ID,
       BRAND_ID,
       STATIC_AMT,
       STATIC_AMT1,
       STATIC_AMT2,
       STATIC_AMT3,
       ORG_ID)
    VALUES
      ('FANSUPPLIERAPPLY', V_RECORD.SUPPLIER_ID, V_RECORD.SUPPLIER_ID, V_RECORD.AMT, NULL, NULL, NULL, NULL);
    COMMIT;
  END LOOP;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE(' - >> SQLCODE ' || SQLCODE || '
         ' || SQLERRM);
END;
//
CREATE OR REPLACE PROCEDURE IM.LOC_PRC_STOCK_WARN
IS
v_sql varchar2(200);
BEGIN

  v_sql := 'truncate table LOC_INV_STOCK_WARN';
  execute immediate v_sql;

  FOR v_region IN (SELECT REGION from region_list t )LOOP
     INSERT INTO LOC_INV_STOCK_WARN
  (REGION,
   INV_ID,
   INV_STATUS,
   CHECK_STATUS,
   STATUS_REASON,
   STATUS_DATE,
   OPER_ID,
   STORE_ID,
   REAL_STORE_ID,
   ORG_ID,
   RES_TYPE_ID,
   PHYSICAL_STATUS,
   CUR_OWNER,
   BUSI_STATUS,
   REAL_COLOR,
   SUPPLIER_ID,
   SETTLE_MODE,
   AGENT_ID,
   SETTLE_STATUS,
   LDSTORE_DATE,
   ORDER_ID,
   OWNER_TYPE)

  SELECT REGION,
         INV_ID,
         INV_STATUS,
         CHECK_STATUS,
         STATUS_REASON,
         STATUS_DATE,
         OPER_ID,
         STORE_ID,
         REAL_STORE_ID,
         ORG_ID,
         RES_TYPE_ID,
         PHYSICAL_STATUS,
         CUR_OWNER,
         BUSI_STATUS,
         REAL_COLOR,
         SUPPLIER_ID,
         SETTLE_MODE,
         AGENT_ID,
         SETTLE_STATUS,
         LDSTORE_DATE,
         ORDER_ID,
         OWNER_TYPE
    FROM im_inv_mobtel t
   WHERE t.supplier_id = 'SD.LU.01.01'
     AND t.settle_mode = 'SELFSALE'
     AND t.owner_type = 'AGENT'
     AND t.region = v_region.REGION
     AND (t.inv_status = 'INSTORE' OR t.inv_status = 'ONWAY')
     AND BUSI_STATUS='USABLE'
     AND T.STATUS_DATE >= TO_DATE('2020-01-01','YYYY-MM-DD');
     COMMIT;
  END LOOP;
  COMMIT;
END;
//

create or replace procedure IM.PROC_PSI_AGENT_DEPOSIT IS
BEGIN
DELETE FROM LOC_AGENT_DEPOSIT WHERE REPORTTYPE='agentReport';
INSERT INTO LOC_AGENT_DEPOSIT
(
REPORTTYPE,
CREATE_DATE,
REGION,
COUNTRYNAME,
ORG_ID,
ORG_NAME,
IS_CHAIN_STORE,
GROUP_ID,
COEFFICIENT,
DEPOSIT,
IS_PURCHASE_HALL,
GROUP_NAME
)
SELECT
'agentReport',
sysdate,
D.REGION,
(SELECT ORGNAME  FROM ORGANIZATION  WHERE ORGID= D.COUNTYID),
D.AGENTID,
(SELECT ORGNAME  FROM ORGANIZATION  WHERE ORGID= D.AGENTID),

DECODE(NVL(D.GROUP_ID, 0), '0', '否', '是')  isChainStore,
D.GROUP_ID,
NVL(D.COEFFICIENT,1),
D.DEPOSIT,
DECODE(D.AGENTID,E.ORG_ID,'是','否'),
(SELECT L.ORG_NAME FROM LOC_WHITE_ORG L WHERE L.BUSI_TYPE='guarantee_group' AND L.ORG_ID = NVL(D.GROUP_ID,0))
 FROM
(SELECT B.REGION,B.COUNTYID,B.AGENTID, C.COEFFICIENT,C.GROUP_ID, DECODE(NVL(C.GROUP_ID, 0), '0', '否', '是')  isChainStore,
(SELECT TO_CHAR(SUM(A.TOTALVAL)) totalVal
              FROM CH_FEE_DEPOSIT_TOTAL A
             WHERE 1 = 1
               AND A.OBJECTID = B.AGENTID
               AND A.OBJECTTYPE = 'cmChannel'
               AND A.STATUS = 'dasActive') deposit
 FROM(
SELECT J.AGENTID,J.REGION,J.COUNTYID  FROM (
SELECT  G.AGENTID,G.REGION,G.COUNTYID FROM (SELECT A.AGENTID,A.REGION,SUBSTR(A.AGENTID,0,8) COUNTYID  FROM AGENT A WHERE A.AGENTTYPE IN (SELECT P.DICT_ID FROM PSI_DICT_ITEM P WHERE P.GROUP_ID = 'LOC_FAN_OGKDAGENT')) G
LEFT JOIN CH_FEE_DEPOSIT_TOTAL F
ON G.AGENTID = F.OBJECTID
WHERE  F.OBJECTTYPE = 'cmChannel'  AND F.STATUS = 'dasActive'
) J  GROUP BY  J.AGENTID,J.REGION,J.COUNTYID
 ) B
LEFT JOIN
(SELECT * FROM  LOC_WHITE_ORG L WHERE L.BUSI_TYPE='deposit_coefficient')  C
ON B.AGENTID = C.ORG_ID
) D
LEFT JOIN(
SELECT * FROM  LOC_WHITE_ORG L WHERE L.BUSI_TYPE='purchase' AND L.SETTLE_MODE='SELFSALE'  AND L.CHANNEL_TYPE='AGENT' AND L.SUPPLIER_ID= 'SD.LU.01.01')  E
ON  D.AGENTID = E.ORG_ID;
UPDATE LOC_AGENT_DEPOSIT L  SET  L.TOTALDEPOSIT = L.DEPOSIT * L.COEFFICIENT;

for v_record in (SELECT L.ORG_ID ,L.IS_PURCHASE_HALL,L.REGION  FROM  LOC_AGENT_DEPOSIT L ) LOOP
IF v_record.is_purchase_hall ='是' THEN
UPDATE LOC_AGENT_DEPOSIT L SET L.STOCKDEPOSIT =(SELECT NVL(SUM(PURCHASE_PRICE),0) FROM IM_INV_MOBTEL T WHERE T.SUPPLIER_ID = 'SD.LU.01.01' AND INV_STATUS <> 'OUTSTORE' AND ORG_ID = v_record.org_id AND T.Settle_Mode = 'SELFSALE' AND region = v_record.region)WHERE L.ORG_ID= v_record.org_id;
UPDATE  LOC_AGENT_DEPOSIT L  SET L.ONWAYDEPOSIT =(SELECT NVL(SUM((NVL(CHECKNUM, APPLY_NUM) - ARRIVE_NUM) * UNIT_PRICE),0)FROM (SELECT A.APPLY_NUM, NVL(A.ARRIVE_NUM, 0) ARRIVE_NUM, B.CHECKNUM, UNIT_PRICE FROM (SELECT * FROM PSI_FLOW_APPLY_BATCH WHERE FLOW_ID IN (SELECT FLOW_ID FROM PSI_FLOW_APPLY T WHERE APPLY_TYPE = 'CATA_MOBAPPLY_PUBLIC_SELL_FAN'AND FLOW_STATUS!='CANCEL' AND T.SUPPLIER_ID =  'SD.LU.01.01'AND ORG_ID = v_record.org_id AND REGION = v_record.region) AND SETTLE_MODE = 'SELFSALE' AND NVL(STATUS,0)!='CANCEL' AND NVL(STATUS,0)!='FINISHED') A  LEFT JOIN LOC_DIRECT_AUDITS_ORDER B ON A.FLOW_ID = B.FLOW_ID AND A.COLOR = B.COLOR AND A.RES_TYPE_ID = B.MODELCODE  WHERE NVL(B.CHECKRESULT,0)='0'))WHERE L.ORG_ID= v_record.org_id;
END IF;
END LOOP;
UPDATE  LOC_AGENT_DEPOSIT L  SET L.REMAINDEPOSIT = NVL(L.TOTALDEPOSIT,0) -  NVL(L.ONWAYDEPOSIT,0) - NVL(L.STOCKDEPOSIT,0)WHERE L.REPORTTYPE = 'agentReport' AND L.IS_PURCHASE_HALL = '是';
dbms_output.put_line(':::更新剩余保证金::::');
commit;
UPDATE LOC_AGENT_DEPOSIT L SET l.is_inv_deposit = 'z',L.INV_DEPOSIT = (SELECT A.COEFFICIENT FROM  LOC_WHITE_ORG A WHERE A.BUSI_TYPE = 'virtual_deposit' AND  L.ORG_ID  = A.ORG_ID AND A.STATUS = '1')WHERE   EXISTS (SELECT 1 FROM LOC_WHITE_ORG A WHERE A.BUSI_TYPE = 'virtual_deposit' AND L.ORG_ID  = A.ORG_ID AND A.STATUS = '1' );
 INSERT INTO LOC_AGENT_DEPOSIT
         SELECT C.REGION,
              (SELECT ORGNAME  FROM ORGANIZATION  WHERE ORGID= C.COUNTYID),
                C.ORG_ID,
               (SELECT ORGNAME  FROM ORGANIZATION  WHERE ORGID=  C.ORG_ID),
                DECODE(NVL(C.GROUP_ID, 0), '0', '否', '是')  isChainStore,
                DECODE(C.ORG_ID,D.ORG_ID,'是','否'),
                C.COEFFICIENT,
                C.COEFFICIENT,
                '',
                SYSDATE,
               '',
                '1',
                '',
                '',
                '',
                'agentReport',
                'y',
                 C.COEFFICIENT
      FROM  (
      SELECT A.ORG_ID,A.ORG_NAME,A.COEFFICIENT,A.REGION,B.GROUP_ID,SUBSTR(A.ORG_ID,0,8) COUNTYID FROM  LOC_WHITE_ORG A
      LEFT JOIN (SELECT * FROM  LOC_WHITE_ORG L WHERE L.BUSI_TYPE='deposit_coefficient')  B
      ON A.ORG_ID =B.ORG_ID
      WHERE  A.BUSI_TYPE = 'virtual_deposit' AND A.STATUS = '1') C
      LEFT JOIN
      (
      SELECT * FROM  LOC_WHITE_ORG L WHERE L.BUSI_TYPE='purchase' AND L.SETTLE_MODE='SELFSALE'  AND L.CHANNEL_TYPE='AGENT' AND L.SUPPLIER_ID= 'SD.LU.01.01'
      ) D
      ON C.ORG_ID = D.ORG_ID;

for v_record1 in (SELECT *  FROM  LOC_AGENT_DEPOSIT E WHERE E.IS_INV_DEPOSIT ='y') LOOP
UPDATE  LOC_AGENT_DEPOSIT L  SET L.STOCKDEPOSIT =(SELECT NVL(SUM(PURCHASE_PRICE),0)  FROM IM_INV_MOBTEL T WHERE T.SUPPLIER_ID = 'SD.LU.01.01' AND INV_STATUS <> 'OUTSTORE' AND ORG_ID=v_record1.org_id  AND T.Settle_Mode = 'SELFSALE' AND region = v_record1.region)WHERE L.ORG_ID= v_record1.org_id;
UPDATE  LOC_AGENT_DEPOSIT L  SET L.ONWAYDEPOSIT =(SELECT NVL(SUM((NVL(CHECKNUM, APPLY_NUM) - ARRIVE_NUM) * UNIT_PRICE),0) FROM (SELECT A.APPLY_NUM, NVL(A.ARRIVE_NUM, 0) ARRIVE_NUM, B.CHECKNUM, UNIT_PRICE FROM (SELECT *  FROM PSI_FLOW_APPLY_BATCH WHERE FLOW_ID IN (SELECT FLOW_ID  FROM PSI_FLOW_APPLY T WHERE APPLY_TYPE = 'CATA_MOBAPPLY_PUBLIC_SELL_FAN'   AND FLOW_STATUS!='CANCEL' AND T.SUPPLIER_ID =  'SD.LU.01.01'AND ORG_ID = v_record1.org_id AND REGION = v_record1.region)AND SETTLE_MODE = 'SELFSALE' AND NVL(STATUS,0)!='CANCEL'  AND NVL(STATUS,0)!='FINISHED') A  LEFT JOIN LOC_DIRECT_AUDITS_ORDER B ON A.FLOW_ID = B.FLOW_ID AND A.COLOR = B.COLOR   AND A.RES_TYPE_ID = B.MODELCODE WHERE NVL(B.CHECKRESULT,0)='0' ))WHERE L.ORG_ID= v_record1.org_id;
END LOOP;
UPDATE  LOC_AGENT_DEPOSIT L  SET L.REMAINDEPOSIT = NVL(L.TOTALDEPOSIT,0) * 100 -  NVL(L.ONWAYDEPOSIT,0) - NVL(L.STOCKDEPOSIT,0)
WHERE L.REPORTTYPE = 'agentReport' AND L.IS_INV_DEPOSIT='y';
dbms_output.put_line(':::更新剩余保证金::::');
commit;
END PROC_PSI_AGENT_DEPOSIT;
//
delimiter ;//