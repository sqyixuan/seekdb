SELECT CBOI.ORDER_NO,
       CPCT.CTP_TYP,
       CPCT.PRO_TYP,
       CBOI.PRO_ID,
       CBOI.TRAN_AMT,
       CBOI.UNIT_MEAT,
       MICUI.ID_TYP BUYER_ID_TYP,
       MICUI.ID_NO BUYER_ID_NO,
       MICUI.CUST_NAME BUYER_CUST_NAME,
       CUI.ID_TYP,
       CUI.ID_NO,
       CUI.CUST_NAME,
       CBOI.CRE_DATE,
       CPCT.STAT,
       ROW_NUMBER() OVER( ORDER BY CBOI.ORDER_NO ) AS RN
  FROM CTP_BT_ORDER_INFO CBOI
  JOIN CTP_PRO_CONFIRM_TR CPCT ON CBOI.CTP_ID = CPCT.CTP_ID
                              AND CBOI.ORDER_NO = CPCT.ORDER_NO
  JOIN CTP_USER_INFO CUI ON CBOI.SELLER_NO = CUI.EBUSER_NO AND CBOI.CTP_ID = CUI.CTP_ID
  JOIN CTP_USER_INFO MICUI ON CBOI.BUYER_NO = MICUI.EBUSER_NO AND CBOI.CTP_ID = MICUI.CTP_ID
  WHERE CBOI.STAT = '3' 
  AND CBOI.CTP_ID='1'
  AND CBOI.CRE_DATE=TO_CHAR(SYSDATE, 'YYYY-MM-DD');
  

SELECT PCT.INTE_SEQ,
        PCT.ACCT_NO,
        CASE WHEN PCT.CHAN_TYPE='3' THEN '1' WHEN PCT.CHAN_TYPE='4' THEN '0' ELSE '2' END TRAN_TYPE,
        TO_NUMBER(PCT.TRANS_AMT) TRANS_AMT,
        PCT.RCV_ACCT,
        PCT.RCV_NAME,
        ROW_NUMBER() OVER(ORDER BY PCT.INTE_SEQ DESC) AS RN
      FROM
        PAYMENT_CHECK_TR PCT, DTB_ACCT DA
      WHERE
        DA.ACCT_NO = PCT.RCV_ACCT
        AND DA.ACCT_TP = '0'
        AND PCT.SUM_INFO!='0005'
        AND DA.CTP_ID='1'
        AND PCT.TRANS_DATE=REPLACE(TO_CHAR(SYSDATE, 'YYYY-MM-DD'),'-','')
       UNION ALL
           SELECT
        PCT.INTE_SEQ,
        PCT.ACCT_NO,
        CASE WHEN PCT.CHAN_TYPE='3' THEN '1' WHEN PCT.CHAN_TYPE='4' THEN '0' ELSE '2' END TRAN_TYPE,
        TO_NUMBER(PCT.TRANS_AMT) TRANS_AMT,
                PCT.RCV_ACCT,
                PCT.RCV_NAME,
                ROW_NUMBER() OVER(ORDER BY PCT.INTE_SEQ DESC) AS RN
            FROM
                PAYMENT_CHECK_TR PCT,CTP_ACCT CA
            WHERE
              CA.CTP_ACCT = PCT.RCV_ACCT
        AND CA.ACCT_STATE='N'
                AND PCT.SUM_INFO!='0005'
                AND CA.CTP_ID='1'
                AND PCT.TRANS_DATE=REPLACE(TO_CHAR(SYSDATE, 'YYYY-MM-DD'),'-','');

SELECT
   CPOT.TRAN_SEQ,
   CPOT.CTP_SEQ TRANS_SEQ,CPOT.CTP_ID,
   CPI.PRO_TYP,
   CPOT.PRO_ID,
   CPI.PRO_NAME,
   TO_CHAR(CPOT.BID_AMT*100) PRO_AMT,
   CPOT.BID_SEQ_NO,
   CPOT.BIDER_ID_TYP ID_TYP,
   CPOT.BIDER_NAME ID_NAME,
   CPOT.BIDER_ID_NO ID_NO,
   CPOT.BID_SEQ_NO,
   CPBT.STAT TRAN_STAT,
   CPBT.RET_CODE HOST_CODE,
   CPOT.SYNZ_URL,
   CPBT.RET_MSG HOST_MSG,
   CUI.CUST_ROLE
FROM CTP_PP_ORDER_TR CPOT
LEFT JOIN CTP_PP_INFO CPI
  ON CPI.CTP_ID=CPOT.CTP_ID
 AND CPI.PRO_ID=CPOT.PRO_ID
LEFT JOIN CTP_PP_CONFIRM_TR CPCT
  ON CPCT.CTP_ID=CPOT.CTP_ID
 AND CPCT.PRO_ID=CPOT.PRO_ID
 AND CPCT.BIDER_NO=CPOT.BIDER_NO
 AND CPCT.BID_SEQ_NO=CPOT.BID_SEQ_NO
LEFT JOIN CTP_PP_BID_TR CPBT
  ON CPBT.TRANS_SEQ=CPCT.PAY_SEQ
LEFT JOIN CTP_USER_INFO CUI
  ON CPCT.BIDER_NO = CUI.EBUSER_NO
 WHERE CPOT.STAT = '4'
   AND CPOT.IS_SYNZ = '0'
   AND CPOT.SYNZ_RATE < 3
   AND CPOT.SYNZ_URL IS NOT NULL
   AND CPBT.STAT IS NOT NULL
   AND ROWNUM <= 10
 ORDER BY CPOT.CRE_DATE, CPOT.CRE_TIME;
             
             
 SELECT CUI.ID_TYP,
       CUI.ID_NO,
       CUI.CUST_NAME ID_NAME,
       CUT.TRAN_SEQ,
       CPABT.CTP_SEQ TRANS_SEQ,
       CUT.SYNZ_URL,
       CUT.CTP_ID,
       CPABT.STAT TRAN_STAT,
       CASE
         WHEN CUT.DATA_FIELD = 'S' THEN
          '1'
         WHEN CUT.DATA_FIELD = 'F' THEN
          '2'
       END AS OPER_TYPE,
       CUI.CUST_ROLE
  FROM CTP_USER_INFO CUI, CTP_URL_TR CUT
  LEFT JOIN CTP_PP_AUTO_BID_TR CPABT 
      ON CPABT.CTP_ID=CUT.CTP_ID
     AND CPABT.CTP_SEQ=CUT.CTP_SEQ
 WHERE CUI.EBUSER_NO = CUT.EBUSER_NO
   AND CUT.CTP_ID = CUI.CTP_ID
   AND CUT.TRAN_TYPE = '6'
   AND CUT.STAT = '2'
   AND CUT.IS_SYNZ != '1'
   AND CUT.SYNZ_RATE < 3
   AND CUT.SYNZ_URL IS NOT NULL
   AND ROWNUM <= 10;
               
               
SELECT A.CTP_SEQ,
       A.PRO_ID,
       A.BID_SEQ_NO,
       C.CUST_NAME,
       A.BUY_ACCT BIDER_ACCT,
       A.BID_AMT,
       A.BUY_AMT TRAN_AMT,
       CASE
         WHEN A.TRAN_TYP = '0' THEN
          '手动投标'
         WHEN A.TRAN_TYP = '1' THEN
          '单笔自动投标'
       END AS BID_TYP,
       B.ACTIVE_ACCT1,
       B.ACTIVE_NAME1,
       B.ACTIVE_AMT1,
       B.ACTIVE_ACCT2,
       B.ACTIVE_NAME2,
       B.ACTIVE_AMT2,
       B.ACTIVE_ACCT3,
       B.ACTIVE_NAME3,
       B.ACTIVE_AMT3,
       TO_CHAR(TO_DATE(A.SUBMIT_TIME, 'YYYY-MM-DD HH24:MI:SS'),
               'YYYY-MM-DD') CRE_DATE,
       TO_CHAR(TO_DATE(A.SUBMIT_TIME, 'YYYY-MM-DD HH24:MI:SS'),
               'HH24:MI:SS') CRE_TIME,
       TO_CHAR(TO_DATE(A.COMP_TIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD') BID_DATE,
       TO_CHAR(TO_DATE(A.COMP_TIME, 'YYYY-MM-DD HH24:MI:SS'), 'HH24:MI:SS') BID_TIME
  FROM CTP_PP_BID_TR A
  LEFT JOIN CTP_PP_ORDER_TR B ON A.CTP_SEQ = B.CTP_SEQ
                             AND A.CTP_ID = B.CTP_ID
                             AND A.PRO_ID = B.PRO_ID
                             AND A.BID_SEQ_NO = B.BID_SEQ_NO
  LEFT JOIN CTP_USER_INFO C ON C.EBUSER_NO = A.BUY_NO
  WHERE A.CTP_ID='1' AND A.STAT='0' AND SUBSTR(COMP_TIME,0,10)=TO_CHAR(SYSDATE, 'YYYY-MM-DD')
UNION ALL
SELECT CTP_SEQ,PRO_ID,
       BID_SEQ BID_SEQ_NO,
       ID_NAME CUST_NAME,
       PAY_ACCT BIDER_ACCT,
       TRAN_AMT BID_AMT,
       TRAN_AMT,
       '批量自动投标' BID_TYP,
       ACTIVE_ACCT1,
       '' ACTIVE_NAME1,
       ACTIVE_AMT1,
       ACTIVE_ACCT2,
       '' ACTIVE_NAME2,
       ACTIVE_AMT2,
       ACTIVE_ACCT3,
       '' ACTIVE_NAME3,
       ACTIVE_AMT3,
       TRAN_DATE CRE_DATE,
       TRAN_TIME CRE_TIME,
       COMPLE_DATE BID_DATE,
       COMPLE_TIME BID_TIME
  FROM CTP_BATCH_AUTOBID_TR
 WHERE STAT = '0'
   AND CTP_ID = '1'
   AND TRAN_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD');
   
SELECT F.CTP_SEQ,
       U.ID_TYP,
       U.ID_NO,
       U.CUST_NAME,
       U.MOBILE,
       S.ACCT_NO,
       C.PACKAGE_ID,
       C.PACKAGE_NAME,
       C.FEE_TOP,
       C.SUB_ACCT_NO,
       C.AMT,
       F.ACTIVE_ACCT1,
       F.ACTIVE_NAME1,
       F.ACTIVE_AMT1,
       F.ACTIVE_ACCT2,
       F.ACTIVE_NAME2,
       F.ACTIVE_AMT2,
       F.ACTIVE_ACCT3,
       F.ACTIVE_NAME3,
       F.ACTIVE_AMT3,
       F.STAT,
       F.TRAN_DATE,
       F.TRAN_TIME,
       C.SIGN_DATE,
       C.SIGN_TIME
  FROM CTP_FINAN_PLAN_INFO C
  LEFT JOIN CTP_FINAN_PLAN_TR F ON F.SUB_ACCT_NO = C.SUB_ACCT_NO AND F.EBUSER_NO = C.EBUSER_NO AND F.PACKAGE_ID = C.Package_Id  
  LEFT JOIN CTP_USER_INFO U ON C.EBUSER_NO = U.EBUSER_NO
  LEFT JOIN CTP_SUB_ACCT S ON C.SUB_ACCT_NO = S.SUB_ACCT_NO
 WHERE F.TRAN_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
   AND F.STAT = '0'
   AND F.CTP_ID = '1' ;
               
SELECT  A.CTP_SEQ,
        A.PRO_ID,
        C.PRO_NAME,
        A.ORI_BID_SEQ_NO,
        A.ZR_NAME,
        A.ZR_ID_TYP,
        A.ZR_ID_NO,
        A.BID_AMT,
        A.TRAN_AMT,
       A.BID_SEQ_NO,
       A.BUY_NAME,
       A.BUY_ID_TYP,
       A.BUY_ID_NO,  
       A.ACTIVE_ACCT1,
       A.ACTIVE_NAME1,
       A.ACTIVE_AMT1,
       A.ACTIVE_ACCT2,
       A.ACTIVE_NAME2,
       A.ACTIVE_AMT2,
       A.FEE_ACCT1,
       A.FEE_NAME1,
       A.FEE_AMT1,
       A.FEE_ACCT2,
       A.FEE_NAME2,
       A.FEE_AMT2,
       A.FEE_ACCT3,
       A.FEE_NAME3,
       A.FEE_AMT3,
       CASE WHEN A.ZR_TYP='0'THEN '部分转让' 
       WHEN A.ZR_TYP='1' THEN '全部转让' 
       END AS ZR_TYP,
       CASE WHEN B.BID_TYP='1' THEN '转让'
        WHEN B.BID_TYP = '4' THEN '自动投债权'
       END AS BID_TYP,
       A.ZR_DATE,
       A.ZR_TIME
 FROM CTP_PP_ZR_ORDER_TR A
 LEFT JOIN CTP_PP_CONFIRM_TR B 
 ON A.CTP_ID=B.CTP_ID
 AND A.PRO_ID=B.PRO_ID
 AND A.BID_SEQ_NO = B.BID_SEQ_NO
 LEFT JOIN CTP_PP_INFO C
 ON A.CTP_ID=C.CTP_ID
 AND A.PRO_ID=C.PRO_ID
 WHERE A.STAT='4'
 AND  B.BID_TYP IN('1','4')
 AND A.CTP_ID='1'
 AND A.CTP_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
 order by a.pro_id;
 
SELECT CBHT.SEQ_NO,
       CBHT.CTP_SEQ,
       CBHT.PRO_ID,
       PPINFO.PRO_NAME,  
       CPCT.BID_SEQ_NO BID_SEQ,  
       CUI.ID_TYP, 
       CUI.ID_NO,
       CUI.CUST_NAME ID_NAME, 
       CASE CBHT.JD_FLAG   WHEN '3' THEN'借' WHEN '4' THEN '贷' END  JD_FLAG, 
       CBHT.TRAN_AMT, 
       CASE CBHT.STAT   WHEN '0' THEN'成功' WHEN '1' THEN '已受理' WHEN '2' THEN '失败' WHEN '4' THEN '部分成功' END STAT,
       CBHT.HOST_MSG,  
       CBHT.TRAN_DATE,
       CBHT.TRAN_TIME,  
       CBHT.COMPLE_DATE,
       CBHT.COMPLE_TIME 
       FROM CTP_BATCH_HOST_TR CBHT 
       LEFT JOIN CTP_PP_CONFIRM_TR CPCT ON CBHT.DETAIL_SEQ = CPCT.CONF_SEQ AND CPCT.BID_STAT NOT IN ('6') AND CPCT.CONF_STAT IN ('10','14') LEFT JOIN CTP_USER_INFO CUI 
       ON CUI.EBUSER_NO = CPCT.BIDER_NO LEFT JOIN CTP_PP_INFO PPINFO ON CBHT.PRO_ID = PPINFO.PRO_ID WHERE CBHT.CTP_ID = '1' AND CBHT.TRAN_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD') 
       AND CBHT.TRAN_TYPE IN('2','3') ORDER BY CBHT.TRAN_DATE,CBHT.TRAN_TIME,CBHT.PRO_ID DESC;
       
               
SELECT A.EACCT_NO ELE_ACCT_NO,
       C.IS_ACTIVE,
       A.SYNZ_URL,
       A.CTP_ID,
       A.UNI_NO,
       A.UNI_STAT,
       A.COMP_NAME ID_NAME,
       A.ID_TYP,
       A.ID_NO,
       B.BANK_NO,
       B.BANK_NAME,
       B.IS_SIGN_MOBILE,
       C.ACCT_NO,
       A.CTP_TRAN_SEQ TRANS_SEQ,
       B.EBUSER_NO,
       TO_CHAR(C.REG_AMT * 100) REG_AMT,
       A.CUST_ROLE,
       C.BANK_NAME BIND_BANK_NAME,
       C.BANK_NO BIND_BANK_NO,
        A.COMP_PHONE MOBILE_NO,
        D.SUB_ACCT_NO,
        D.SUB_ACCT_NO_SECOND 
  FROM CTP_COMP_QUAIL_INFO A 
  LEFT JOIN DTB_ACCT B ON A.EACCT_NO = B.ACCT_NO
  LEFT JOIN DTB_ACCT C ON B.EBUSER_NO = C.EBUSER_NO AND C.ACCT_TP <> '0' 
  LEFT JOIN DTB_ACCT_EXTEND D ON B.EBUSER_NO = D.EBUSER_NO 
 WHERE A.IS_OPEN_SYNC = 'N'
   AND A.SYNZ_URL IS NOT NULL
   AND A.SYNZ_OPEN_RATE < 3
   AND B.IS_ACTIVE = '1'
   AND A.EACCT_TYPE = '0'
   AND ROWNUM <= 10;
               
SELECT A.EACCT_NO ELE_ACCT_NO,
       B.IS_ACTIVE,
       A.SYNZ_URL,
       A.CTP_ID,
       A.UNI_NO,
       A.UNI_STAT,
       A.COMP_NAME ID_NAME,
       A.ID_TYP,
       A.ID_NO,
       B.BANK_NO,
       B.BANK_NAME,
       B.IS_SIGN_MOBILE,
       C.ACCT_NO,
       A.CTP_TRAN_SEQ TRANS_SEQ,
       B.EBUSER_NO,
       TO_CHAR(C.REG_AMT * 100) REG_AMT
  FROM CTP_COMP_QUAIL_INFO A 
  LEFT JOIN DTB_ACCT B ON A.EACCT_NO = B.ACCT_NO
  LEFT JOIN DTB_ACCT C ON B.EBUSER_NO = C.EBUSER_NO AND C.ACCT_TP <> '0'
 WHERE A.SL_OPEN_SYNC = 'N'
   AND A.SYNZ_URL IS NOT NULL
   AND A.SL_OPEN_DATE < 3
   AND B.IS_ACTIVE = '1'
   AND A.EACCT_TYPE = '0'
   AND ROWNUM <= 10;


SELECT A.SYNZ_URL,A.DATA_FIELD ACCT_NO,C.ID_NO,A.CTP_ID,C.CUST_NAME ID_NAME,B.IS_ACTIVE,A.TRAN_SEQ,
    B.BANK_NO BANK_CODE,B.BANK_NAME,B.REG_AMT*100 REG_AMT,A.TRAN_TYPE,C.ID_TYP,A.CTP_SEQ TRANS_SEQ,
            CASE
            WHEN D.OPER_RESULT = '1' THEN
              '0'
          END AS TRAN_STAT,A.CUST_ROLE  
    FROM CTP_URL_TR A
    LEFT JOIN DTB_ACCT B
    ON A.EBUSER_NO = B.EBUSER_NO AND B.ACCT_TP <> '0' AND A.DATA_FIELD = B.ACCT_NO
    LEFT JOIN CTP_USER_INFO C
    ON C.EBUSER_NO = A.EBUSER_NO
    LEFT JOIN DTB_BIND_CARD_TR D
    ON D.ORDER_NO = A.TRAN_SEQ
    WHERE A.IS_SYNZ = 'N' 
      AND A.SYNZ_URL IS NOT NULL 
      AND A.SYNZ_RATE < 3 
      AND A.TRAN_TYPE in ('1','2')
      AND D.OPER_RESULT = '1';
              
              
SELECT  CASE WHEN DU.OPER_TYPE='0'
      THEN COUNT(DU.TRAN_AMT)*PI.FEE_IN_AMT*100
    WHEN DU.OPER_TYPE='1'
      THEN COUNT(DU.TRAN_AMT)*PI.FEE_OUT_AMT*100
    END AS FEE_COST,
  SUM(DR.TRAN_FEE)*100 TOT_FEE,SUM(DUL.OPER_AMT)*100 TOT_AMT,
  DR.GG_BRANCH_NO,DU.OPER_TYPE,
  DR.GG_TELLER_NO,DR.TERMINALID,
  DU.PASS_TYPE,DR.CTP_ID,DR.CHAN_TYP,
      DU.LOAD_DATE,DU.ORDER_DATE
FROM DTB_UPAY_ACCT_CHECK DU
LEFT JOIN DTB_ROLL_TR DR ON DR.TRAN_SEQ=DU.TRAN_SEQ
LEFT JOIN PASS_INF PI ON PI.PASS_TYPE='1'
LEFT JOIN DTB_UPAY_LIST DUL ON DUL.TRAN_SEQ = DU.TRAN_SEQ
WHERE 1=1
  AND DU.CHECK_RESULT = '3'
  AND DU.PASS_TYPE = '1'
  AND DU.ORDER_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
GROUP BY DR.GG_BRANCH_NO,DU.OPER_TYPE,
    DU.PASS_TYPE,DU.LOAD_DATE,DU.ORDER_DATE,
    PI.FEE_IN_AMT,PI.FEE_OUT_AMT,DR.CTP_ID,
    DR.CHAN_TYP,DR.GG_TELLER_NO,DR.TERMINALID;
    
                
                
select count(1)
    FROM CTP_COMP_QUAIL_INFO A
  LEFT JOIN DTB_ACCT B ON A.CUSTOMERID = B.EBUSER_NO AND B.ACCT_TP <> '0'
  WHERE A.CTP_ID ='1'
  AND A.IS_ACTIVE_SYNC='N'
  AND A.SYNZ_ACTIVE_RATE='3';
              
#SELECT COUNT(A.TRAN_SEQ)
#FROM  E_OPER_MESSAGE_QUEUE A
#LEFT JOIN E_AUTH_INFO_TR C ON C.TRAN_SEQ = A.TRAN_SEQ
#LEFT JOIN E_AUTH_QUEUE D ON D.QUEUE_NO = A.QUEUE_NO
#WHERE 
#    A.MENU_ID IN (
#        SELECT MENU_ID FROM E_AUTH_QUEUE_GROUP B
#        WHERE B.GR_ID = '1'
#      )
#   AND A.EBUSER_NO = '1'
#   AND A.OPER_ID = '1';
               
               
               
DELETE FROM E_AUTH_TEMPLATE_REL WHERE  EBUSER_NO='1' AND CHAN_TYP='1';
             
INSERT INTO E_AUTH_TEMPLATE_REL (EBUSER_NO, AUTH_TEMPLATE_ID,CHAN_TYP,MENU_GR_ID) VALUES( '1', '2', '1', '2' );
 
UPDATE E_AUTH_TEMPLATE SET SET_ALL = '2' WHERE EBUSER_NO = '1';
              
              
SELECT trans_code,
        count(trans_code)AS TO_NUM_PEN,
        count(CASE  WHEN H_RSP_CODE = '0000' THEN 'gc' else null end) AS FR_SUCC,
        count(CASE  WHEN H_RSP_CODE != '0000' OR H_RSP_CODE IS NULL THEN 'sb' else null end) AS FR_FAIL,
        (concat(round(count(CASE  WHEN H_RSP_CODE = '0000' THEN 'gc' else null end)/count(trans_code)*100,2),'%')) as PROPORTION,
            ROW_NUMBER() OVER(ORDER BY trans_code DESC) AS RN 
from ctp_common_tr  
WHERE  tran_date =TO_CHAR(SYSDATE, 'YYYY-MM-DD')  group by trans_code ;

               
SELECT trans_code,
        count(trans_code)AS TO_NUM_PEN,
        count(CASE  WHEN H_RSP_CODE = '0000' THEN 'gc' else null end) AS FR_SUCC,
        count(CASE  WHEN H_RSP_CODE != '0000' OR H_RSP_CODE IS NULL THEN 'sb' else null end) AS FR_FAIL,
        (concat(round(count(CASE  WHEN H_RSP_CODE = '0000' THEN 'gc' else null end)/count(trans_code)*100,2),'%')) as PROPORTION,
            ROW_NUMBER() OVER(ORDER BY trans_code DESC) AS RN 
from ctp_common_tr group by trans_code ;
               
               
select H_RSP_CODE ,
        sum(1) as RSP_SUM,
        (concat(round(sum(1) /(select  sum(1) from ctp_common_tr b where  B.H_RSP_CODE != '0000' OR B.H_RSP_CODE IS NULL)*100,2),'%')) as RSP_PROPORTION,
        ROW_NUMBER() OVER(ORDER BY a.H_RSP_CODE DESC) AS RN
from ctp_common_tr a  
where ( a.H_RSP_CODE != '0000' OR a.H_RSP_CODE IS NULL)
group by H_RSP_CODE;
               
               
select count(*) 
from  (select tran_date,count(1),sum(oper_amt) 
		from dtb_roll_tr  
		where  oper_type = '0' and tran_state='6'
		group by tran_date
);
             
             
select tran_date,count(1) as TRAN_NUM ,sum(oper_amt) as TRAN_AMT,ROW_NUMBER() OVER(ORDER BY tran_date DESC) AS RN from dtb_roll_tr
where  oper_type = '0' and tran_state='6'
group by tran_date;
             

select ORDER_DATE, totnum, FAILNUM, rownum rn
from (select ORDER_DATE, max(totnum) totnum, max(FAILNUM) FAILNUM
from (select a.order_date,
           a.TOTNUM,
           B.FAILNUM,
           ROW_NUMBER() OVER(ORDER BY a.ORDER_DATE DESC) AS RN
      from (SELECT COUNT(ORDER_DATE) TOTNUM,
                   to_char(to_date(ORDER_DATE, 'yyyy-mm-dd'),
                           'yyyy-mm-dd') order_date
              FROM REALNAME_CHECK_TR
             where 1 = 1
               and PASS_TYPE = '2'
               AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
                   TO_DATE('2012-12-01', 'YYYY-MM-DD')
              AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
                   TO_DATE('2012-12-01', 'YYYY-MM-DD')
             GROUP BY ORDER_DATE
             ORDER BY ORDER_DATE DESC) a
      left join (SELECT COUNT(ORDER_DATE) FAILNUM,
                       to_char(to_date(ORDER_DATE, 'yyyy-mm-dd'),
                               'yyyy-mm-dd') order_date
                  FROM REALNAME_CHECK_TR
                 where 
                  check_result !='0'
                 and PASS_TYPE = '2'
                   AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
                       TO_DATE('2012-12-01', 'YYYY-MM-DD')
                   AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
                      TO_DATE('2012-12-01', 'YYYY-MM-DD')
                 GROUP BY ORDER_DATE
                 ORDER BY ORDER_DATE DESC) b on a.ORDER_DATE = b.ORDER_DATE
    union
    select ORDER_DATE, totnum, FAILNUM, rn
     from (select to_char(TO_DATE('2012-12-01', 'yyyy-mm-dd') +
                           (level - 1),
                           'yyyy-mm-dd') ORDER_DATE,
                   0 totnum,
                   0 FAILNUM,
                   rownum rn
              from dual
            connect by trunc(TO_DATE('2012-12-01', 'yyyy-mm-dd')) +
                       level - 1 <=
                       trunc(TO_DATE('2012-12-01', 'yyyy-mm-dd'))))
group by order_date
order by order_date);
         
         
#OBE-00942: Table 'ZJCG.REALNAME_CHECK_TR' doesn't exist         
select a.ORDER_DATE,a.TOTNUM,B.FAILNUM,ROW_NUMBER() OVER(ORDER BY a.ORDER_DATE DESC) AS RN
from (SELECT COUNT(ORDER_DATE) TOTNUM, ORDER_DATE
      FROM REALNAME_CHECK_TR
     where 1 = 1
       and PASS_TYPE = '1'
       AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
           TO_DATE('2012-12-01', 'YYYY-MM-DD')
       AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
           TO_DATE('2012-12-01', 'YYYY-MM-DD')
     GROUP BY ORDER_DATE, IS_CHECK
     ORDER BY ORDER_DATE DESC) a
left join (SELECT COUNT(ORDER_DATE) FAILNUM, ORDER_DATE
           FROM REALNAME_CHECK_TR
          where CHECK_RESULT != '0'
            and PASS_TYPE = '2'
            AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
                TO_DATE('2012-12-01', 'YYYY-MM-DD')
            AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
                TO_DATE('2012-12-01', 'YYYY-MM-DD')
          GROUP BY ORDER_DATE, IS_CHECK
          ORDER BY ORDER_DATE DESC) b on a.ORDER_DATE = b.ORDER_DATE;
          

select ORDER_DATE, totnum, FAILNUM
from (select ORDER_DATE, max(totnum) totnum, max(FAILNUM) FAILNUM
from (select a.order_date,
           a.TOTNUM,
           B.FAILNUM,
           ROW_NUMBER() OVER(ORDER BY a.ORDER_DATE DESC) AS RN
      from (SELECT COUNT(ORDER_DATE) TOTNUM,
                   to_char(to_date(ORDER_DATE, 'yyyy-mm-dd'),
                           'yyyy-mm-dd') order_date
              FROM REALNAME_CHECK_TR
             where 1 = 1
               and PASS_TYPE = '2'
               AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
                   TO_DATE('2012-12-01', 'YYYY-MM-DD')
               AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
                   TO_DATE('2012-12-01', 'YYYY-MM-DD')
             GROUP BY ORDER_DATE
             ORDER BY ORDER_DATE DESC) a
     left join (SELECT COUNT(ORDER_DATE) FAILNUM,
                       to_char(to_date(ORDER_DATE, 'yyyy-mm-dd'),
                               'yyyy-mm-dd') order_date
                  FROM REALNAME_CHECK_TR
                where 
                  check_result !='0'
                 and PASS_TYPE = '2'
                   AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') >=
                       TO_DATE('2012-12-01', 'YYYY-MM-DD')
                   AND TO_DATE(ORDER_DATE, 'YYYY-MM-DD') <=
                       TO_DATE('2012-12-01', 'YYYY-MM-DD')
                 GROUP BY ORDER_DATE
                 ORDER BY ORDER_DATE DESC) b on a.ORDER_DATE =
                                                b.ORDER_DATE
    union
    select ORDER_DATE, totnum, FAILNUM, rn
      from (select to_char(TO_DATE('2012-12-01', 'yyyy-mm-dd') +
                           (level - 1),
                           'yyyy-mm-dd') ORDER_DATE,
                   0 totnum,
                   0 FAILNUM,
                   rownum rn
              from dual
            connect by trunc(TO_DATE('2012-12-01', 'yyyy-mm-dd')) +
                       level - 1 <=
                       trunc(TO_DATE('2012-12-01', 'yyyy-mm-dd'))))
group by order_date
order by order_date);
         
         
select ORDER_DATE, totnum, FAILNUM, rn
from (select to_char(TO_DATE('2012-12-01',
                           'yyyy-mm-dd') + (level - 1),
                   'yyyy-mm-dd') ORDER_DATE,
           0 totnum,
           0 FAILNUM,
           rownum rn
      from dual
    connect by trunc(TO_DATE('2012-12-01',
                             'yyyy-mm-dd')) + level - 1 <=
               trunc(TO_DATE('2012-12-01',
                             'yyyy-mm-dd')));
                             

SELECT A.*,B.ID_NO,B.ID_TYP,B.CUST_NAME ID_NAME,B.EBUSER_NO,C.ACCT_NO ELE_ACCT_NO,
            CASE WHEN B.ID_TYP='01' THEN '身份证'
                WHEN B.ID_TYP='02' THEN '户口本'
                WHEN B.ID_TYP='03' THEN '军官证'
                WHEN B.ID_TYP='05' THEN '护照'
                WHEN B.ID_TYP='06' THEN '营业执照'
                WHEN B.ID_TYP='07' THEN '开户许可证'
                WHEN B.ID_TYP='08' THEN '税务登记证'
                WHEN B.ID_TYP='09' THEN '纳税人编码'
                WHEN B.ID_TYP='20' THEN '港澳居民来往内地通行证'
                WHEN B.ID_TYP='21' THEN '台湾居民来往大陆通行证'
                WHEN B.ID_TYP='22' THEN '临时身份证'
                WHEN B.ID_TYP='23' THEN '武警证'
                WHEN B.ID_TYP='24' THEN '外国人永久居留证'
                WHEN B.ID_TYP='25' THEN '边民出入通行证'
                WHEN B.ID_TYP='26' THEN '外国护照' 
            END AS ID_TYP_CNY,
            B.CUST_ROLE,
            A.TRAN_DATE,
            A.TRAN_TIME
            FROM CTP_PP_AUTO_COMPENT_TR A 
            LEFT JOIN CTP_USER_INFO B ON A.EBUSER_NO = B.EBUSER_NO
            LEFT JOIN DTB_ACCT C ON A.EBUSER_NO = C.EBUSER_NO
            WHERE A.TRAN_SEQ = '1'
            AND A.CTP_ID = '2'
            AND A.TRAN_STAT='3'
            AND C.ACCT_TP='0';