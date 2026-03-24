delimiter //;
CREATE OR REPLACE FUNCTION IM.EXECLECEILING(channeltype IN VARCHAR2,price IN NUMBER)
RETURN NUMBER
IS

       v_cheng NUMBER;
       v_chu  NUMBER;
       v_shang  NUMBER;
       v_result   NUMBER;

       BEGIN

         IF channeltype !='AGENT' THEN

           SELECT TO_NUMBER(PARAM_VALUE) INTO v_cheng  FROM PSI_PARAM_VALUE T WHERE PARAM_ID = 'EXECLECEILING_SELF_CE' AND  RES_TYPE_ID='rsclM' AND RES_KIND_ID='rsclM' AND ORG_ID='SD';

           SELECT TO_NUMBER(PARAM_VALUE) INTO v_chu  FROM PSI_PARAM_VALUE T WHERE PARAM_ID = 'EXECLECEILING_SELF_CU' AND  RES_TYPE_ID='rsclM' AND RES_KIND_ID='rsclM' AND ORG_ID='SD';

           SELECT round(price*v_cheng/v_chu,3)  INTO v_shang FROM DUAL;

           SELECT  v_shang-MOD(v_shang,5) INTO v_result  FROM DUAL;

         ELSE

           SELECT TO_NUMBER(PARAM_VALUE) INTO v_cheng  FROM PSI_PARAM_VALUE T WHERE PARAM_ID = 'EXECLECEILING_AGENT_CE' AND  RES_TYPE_ID='rsclM' AND RES_KIND_ID='rsclM' AND ORG_ID='SD';
           SELECT TO_NUMBER(PARAM_VALUE) INTO v_chu  FROM PSI_PARAM_VALUE T WHERE PARAM_ID = 'EXECLECEILING_AGENT_CU' AND  RES_TYPE_ID='rsclM' AND RES_KIND_ID='rsclM' AND ORG_ID='SD';
           SELECT round(price/v_cheng*v_chu,0)  INTO v_result FROM DUAL;
         END IF;

        RETURN(v_result);

        EXCEPTION

            WHEN OTHERS THEN

            RETURN NULL;

END EXECLECEILING;
//

CREATE OR REPLACE PACKAGE IM.P_LOC_ALLOT_INLET IS

  PROCEDURE P_LOC_ALLOT_INLET_OUTLINE(v_region  IN NUMBER,
                                      insysdate IN VARCHAR2);
  PROCEDURE P_LOC_ALLOT_INLET_M_INSERT(v_kind_type IN VARCHAR2,
                                       v_table1     IN VARCHAR2,
                                       v_region    IN NUMBER,
                                       insysdate   IN VARCHAR2,
                                       v_table2     IN VARCHAR2,
                                       v_org_id     IN VARCHAR2);

END P_LOC_ALLOT_INLET;
//

CREATE OR REPLACE PACKAGE BODY IM.P_LOC_ALLOT_INLET IS

  PROCEDURE P_LOC_ALLOT_INLET_OUTLINE(v_region  IN NUMBER,
                                      insysdate IN VARCHAR2) IS
  BEGIN
    --根据时间和地市清除中间表数据
    DELETE FROM LOC_ALLOT_INLET_MID t
     WHERE t.CREATE_TIME = insysdate AND t.region=v_region;

    --sim卡调拨入库数据插入中间表(im_inv_imsi,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'SD.LE.01.01.s4');
    --sim卡调拨入库数据插入中间表(im_inv_imsi,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'SD.LP.01.05.02');
    --sim卡调拨入库数据插入中间表(im_inv_imsi,出入库明细表,默认渠道default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'default');
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'SD.LE.01.01.s4');
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'SD.LP.01.05.02');
     --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细表,default)
     P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'default');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'SD.LE.01.01.s4');
     --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'SD.LP.01.05.02');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'default');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细历史表,'SD.LE.01.01.s4')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'SD.LE.01.01.s4');
    --sim卡调拨入库数据插入中间表(im_inv_imsi,出入库明细历史表,'特殊渠道SDSD.LP.01.05.02')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'SD.LP.01.05.02');
    --sim卡调拨入库数据插入中间表(im_inv_imsi,出入库明细历史表,'default')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi',
                               'default');
     --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细历史表,'特殊渠道SD.LE.01.01.s4')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'SD.LE.01.01.s4');
     --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细历史表,'特殊渠道SD.LP.01.05.02')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'SD.LP.01.05.02');
     --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细历史表,'default')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'default');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细历史表,'特殊渠道SD.LE.01.01.s4')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'SD.LE.01.01.s4');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细历史表,'特殊渠道SD.LP.01.05.02')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'SD.LP.01.05.02');
    --sim卡调拨入库数据插入中间表(im_inv_imsi_his,出入库明细历史表,'default')
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_imsi_his',
                               'default');

   --有价卡调拨入库数据插入中间表
   --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'SD.LE.01.01.s4');
    --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'SD.LP.01.05.02');
    --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细表,特殊渠道default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'default');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细表,特殊渠道SD.LE.01.01.s4)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'SD.LE.01.01.s4');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'SD.LP.01.05.02');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'default');
    --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细表,特殊渠道SD.LE.01.01.s4)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'SD.LE.01.01.s4');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细表,特殊渠道SD.LP.01.05.02)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'SD.LP.01.05.02');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细表,default)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'default');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'SD.LE.01.01.s4');
   --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细历史表,特殊渠道SD.LP.01.05.02)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'SD.LP.01.05.02');
   --有价卡调拨入库数据插入中间表(im_inv_reinforce,出入库明细历史表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce',
                               'default');
    --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'SD.LE.01.01.s4');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细历史表,特殊渠道SD.LP.01.05.02)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'SD.LP.01.05.02');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_use,出入库明细历史表,default)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_use',
                               'default');
    --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'SD.LE.01.01.s4');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细历史表,特殊渠道SD.LP.01.05.02)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'SD.LP.01.05.02');
     --有价卡调拨入库数据插入中间表(im_inv_reinforce_his,出入库明细历史表,default)
   P_LOC_ALLOT_INLET_M_INSERT('rsclR',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_reinforce_his',
                               'default');
    --空白卡调拨入库数据插入中间表
    --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'SD.LE.01.01.s4');
     --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'SD.LP.01.05.02');
     --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'default');
   --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                                'SD.LE.01.01.s4');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                                'SD.LP.01.05.02');
    --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                                'default');
    --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细表,特殊渠道SD.LE.01.01.s4)
  P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                               'SD.LE.01.01.s4');
    --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                               'SD.LP.01.05.02');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                               'default');
    --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'SD.LE.01.01.s4');
      --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细历史表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'SD.LP.01.05.02');
    --空白卡调拨入库数据插入中间表(im_inv_realsign,出入库明细历史表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign',
                               'default');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                               'SD.LE.01.01.s4');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细历史表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                               'SD.LP.01.05.02');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_use,出入库明细历史表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_use',
                               'default');
    --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细历史表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                                'SD.LE.01.01.s4');
    --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细历史表,特殊渠道SD.LP.01.05.02)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                                'SD.LP.01.05.02');
     --空白卡调拨入库数据插入中间表(im_inv_realsign_his,出入库明细历史表,default)
    P_LOC_ALLOT_INLET_M_INSERT('rsclW',
                               'IM_INOUT_STORE_DETAIL_HIS',
                               v_region,
                               insysdate,
                               'im_inv_realsign_his',
                                'default');

   /* --调拨中间表汇总到调拨库表
    P_LOC_ALLOT_INLET_INSERT(insysdate);*/
    COMMIT;
  END P_LOC_ALLOT_INLET_OUTLINE;

  --SIM卡汇总数据到销售出库中间表
  PROCEDURE P_LOC_ALLOT_INLET_M_INSERT(v_kind_type IN VARCHAR2,
                                       v_table1     IN VARCHAR2,
                                       v_region    IN NUMBER,
                                       insysdate   IN VARCHAR2,
                                       v_table2     IN VARCHAR2,
                                       v_org_id     IN VARCHAR2) IS
    v_sql VARCHAR(6096);
    v_hint1 VARCHAR(60);
    v_hint2 VARCHAR(60);
  BEGIN

  if v_table1='IM_INOUT_STORE_DETAIL' then
    v_hint1:=' index(sde IDX_IMINOUTDETAILBUSI) ';
  elsif v_table1='IM_INOUT_STORE_DETAIL_HIS' then
    v_hint1:=' index(sde PK_IMINOUTSTOREDETAIL_HIS) ';
  else
    v_hint1:=' ';
  end if;

  if v_table2='im_inv_imsi' then
       v_hint2:=' index(IMU PK_IMINVIMSI) ';
  elsif v_table2='im_inv_imsi_use' then
       v_hint2:=' index(IMU PK_IMINVIMSIUSE) ';
  elsif v_table2='im_inv_imsi_his' then
       v_hint2:=' index(IMU PK_IMINVIMSIHIS) ';
  elsif v_table2='im_inv_reinforce' then
       v_hint2:=' index(IMU PK_IMINVREINFORCE) ';
  elsif v_table2='im_inv_reinforce_use' then
       v_hint2:=' index(IMU PK_IMINVREINFORCEUSE) ';
  elsif v_table2='im_inv_realsign' then
       v_hint2:=' index(IMU PK_IMINVREALSIGN) ';
  elsif v_table2='im_inv_realsign_use' then
       v_hint2:=' index(IMU PK_IMINVREALSIGNUSE) ';
  elsif v_table2='im_inv_realsign_his' then
       v_hint2:=' index(IMU PK_IMINVREALSIGNHIS) ';
  elsif v_table2='im_inv_reinforce_his' then
       v_hint2:=' index(IMU PK_IMINVREINFORCEHIS) ';
  else
      v_hint2:=' ';
  end if;

    v_sql := 'insert into LOC_ALLOT_INLET_MID(CREATE_TIME,ORGANIZATION_CODE_OUT,ORGANIZATION_CODE_IN,ALLOT_TIME
    ,MATERIAL_CODE,ERP_BATCH_ID,ALLOT_STAFF,region,OUID)
        select/*+leading(bo bop) index(bo IDX_IM_BUSI_ENDDATE) '||v_hint1||v_hint2||'*/
         TO_CHAR(bo.end_date,''yyyymmdd'') CREATE_TIME,';
    IF  v_org_id = 'SD.LE.01.01.s4' THEN
      v_sql := v_sql ||'(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(bop.org_id, 1, 14),
              '' SD.LE.01.01.s4 '',
              '' SD.LE.01.01.s4 '',
              substr(bop.org_id,
                     1,
                     decode(instr(bop.org_id, '' . '', 1, 3),
                            0,
                            length(bop.org_id),
                            instr(bop.org_id, '' . '', 1, 3) - 1)))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_OUT,
              (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(bo.org_id, 1, 14),
              '' SD.LE.01.01.s4 '',
              '' SD.LE.01.01.s4 '',
              substr(bo.org_id,
                     1,
                     decode(instr(bo.org_id, '' . '', 1, 3),
                            0,
                            length(bo.org_id),
                            instr(bo.org_id, '' . '', 1, 3) - 1)))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_IN,';
    ELSE
         IF v_org_id = 'SD.LP.01.05.02' THEN
                 v_sql := v_sql ||'(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(bop.org_id,
              ''SD.LP.01.05'',
              ''SD.LP.01.05'',
              decode(substr(bop.org_id, 1, 14),
                     ''SD.LP.01.05.02'',
                     ''SD.LP.01.05.02'',
                     substr(bop.org_id,
                            1,
                            decode(instr(bop.org_id, ''.'', 1, 3),
                                   0,
                                   length(bop.org_id),
                                   instr(bop.org_id, ''.'', 1, 3) - 1))))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_OUT,
              (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(bo.org_id,
              ''SD.LP.01.05'',
              ''SD.LP.01.05'',
              decode(substr(bo.org_id, 1, 14),
                     ''SD.LP.01.05.02'',
                     ''SD.LP.01.05.02'',
                     substr(bo.org_id,
                            1,
                            decode(instr(bo.org_id, ''.'', 1, 3),
                                   0,
                                   length(bo.org_id),
                                   instr(bo.org_id, ''.'', 1, 3) - 1))))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_IN,';
           ELSE
              v_sql := v_sql ||'(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = substr(bop.org_id,1,decode(instr(bop.org_id,''.'',1,3),0,length(bop.org_id),instr(bop.org_id,''.'',1,3)-1))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_OUT,
              (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = substr(bo.org_id,1,decode(instr(bo.org_id,''.'',1,3),0,length(bo.org_id),instr(bo.org_id,''.'',1,3)-1))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') ORGANIZATION_CODE_IN,';
           END IF;
    END IF;
     v_sql :=v_sql ||'TO_CHAR(bo.end_date,''yyyymmdd'') ALLOT_TIME,
           (SELECT ATT.ATTR_VALUE
             FROM IM_DICT.IM_RES_TYPE_ATTR_SET ATT
            WHERE ATT.ATTR_ID = ''ScmMaterialCode''
              AND sde.RES_TYPE_ID = ATT.RES_TYPE_ID) MATERIAL_CODE,
               IMU.ERP_BATCH_ID,
              (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(bop.org_id,1,5),''SD.LP'',''634'',''SD.LA'',''531'',bop.Region)
              AND SE.FUNC_TYPE = ''SCM_OASTAFF''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OA''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OA'')  ALLOT_STAFF,
              bop.Region,
              (SELECT SE.DES_VALUE
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(bop.org_id,1,5),''SD.LP'',''634'',''SD.LA'',''531'',bop.Region)
              AND SE.FUNC_TYPE = ''SCM_OUID''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OUID''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OUID'') OUID
              FROM im_busi_opera bop,im_busi_opera bo,' ||
             v_table1 || ' sde,' ||v_table2 || ' IMU
    WHERE bo.busi_type = ''ASIGIN''
      AND bo.status = ''CLOSED''
      AND bo.region = '||v_region||'
      AND bo.res_kind_id='''||v_kind_type||'''
      AND bo.end_date BETWEEN TO_DATE('||insysdate||',''yyyymmdd'')
      AND TO_DATE('||insysdate||',''yyyymmdd'')+1
      AND bo.busi_id=sde.busi_id
      AND sde.region='||v_region||'
      AND bop.busi_type = ''ASIGOUT''
      AND bop.status = ''SUCCESS''
      AND bop.region='||v_region||'
      AND IMU.region='||v_region||'
      AND bo.rela_busi_id = bop.busi_id
      AND bop.res_kind_id='''||v_kind_type||'''';

      IF v_org_id = 'SD.LE.01.01.s4' THEN
          v_sql := v_sql ||'AND ((substr(bop.org_id,1,14)=''SD.LE.01.01.s4'')AND (substr(bo.org_id,1,14)<>''SD.LE.01.01.s4'')
                                OR (substr(bo.org_id,1,14)=''SD.LE.01.01.s4'')AND (substr(bop.org_id,1,14)<>''SD.LE.01.01.s4''))';
      ELSE
          IF v_org_id = 'SD.LP.01.05.02' THEN
             v_sql := v_sql ||'AND((bo.org_id =''SD.LP.01.05''AND bop.org_id<>''SD.LP.01.05'')
                                  OR(bop.org_id =''SD.LP.01.05''AND bo.org_id<>''SD.LP.01.05'')
                                  OR(bo.org_id like ''SD.LP.01.05.02%''AND substr(bop.org_id,1,14)<>''SD.LP.01.05.02'')
                                  OR(bop.org_id like ''SD.LP.01.05.02%''AND substr(bo.org_id,1,14)<>''SD.LP.01.05.02''))';
          ELSE
             v_sql := v_sql ||'AND (substr(bo.org_id,1,decode(instr(bo.org_id,''.'',1,3),0,length(bo.org_id),instr(bo.org_id,''.'',1,3)-1))
             <> substr(bop.org_id,1,decode(instr(bop.org_id,''.'',1,3),0,length(bop.org_id),instr(bop.org_id,''.'',1,3)-1)))
             AND substr(bo.org_id,1,14)<>''SD.LE.01.01.s4''
             AND substr(bop.org_id,1,14)<>''SD.LE.01.01.s4''
             AND bo.org_id<>''SD.LP.01.05''
             AND bop.org_id<>''SD.LP.01.05''
             AND substr(bo.org_id,1,14)<>''SD.LP.01.05.02''
             AND substr(bop.org_id,1,14)<>''SD.LP.01.05.02''';
      END IF;
      END IF;

      --取值数据所属资源类型
    IF v_kind_type = 'rsclS' THEN
      v_sql := v_sql || ' AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                        WHERE sde.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclS'')
                          AND sde.Inv_id=IMU.Inv_id';
    ELSE
      IF v_kind_type = 'rsclR' THEN
        v_sql := v_sql ||
                 ' AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                         WHERE sde.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclR'')
                              AND sde.Inv_id=IMU.Inv_id';
      ELSE
        v_sql := v_sql ||
                 'AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                         WHERE sde.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclW'')
                              AND sde.Inv_id=IMU.Inv_id';
      END IF;
    END IF;
    EXECUTE IMMEDIATE v_sql;
    COMMIT;
     EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;

   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);

  END P_LOC_ALLOT_INLET_M_INSERT;
END P_LOC_ALLOT_INLET;
//

CREATE OR REPLACE PACKAGE IM.P_LOC_SALES_OUTLET IS

  PROCEDURE P_LOC_SALES_OUTLET_OUTLINE(v_region  IN NUMBER,
                                       insysdate IN VARCHAR2);
  PROCEDURE P_LOC_SALES_OUTLET_M_INSERT(v_sale_status  IN VARCHAR2,
                                        v_open         IN VARCHAR2,
                                        v_table        IN VARCHAR2,
                                        v_card_type    IN VARCHAR2,
                                        v_card_type_id IN VARCHAR2,
                                        v_region       IN NUMBER,
                                        insysdate      IN VARCHAR2);
 
END P_LOC_SALES_OUTLET;
//

CREATE OR REPLACE PACKAGE BODY IM.P_LOC_SALES_OUTLET IS

  PROCEDURE P_LOC_SALES_OUTLET_OUTLINE(v_region  IN NUMBER,
                                       insysdate IN VARCHAR2) IS
  BEGIN

    DELETE FROM LOC_SALES_OUTLET_MID t
     WHERE t.CREATE_TIME = insysdate AND t.region=v_region;

    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_IMSI',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_IMSI_USE',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_IMSI_HIS',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_IMSI',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_IMSI_USE',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_IMSI_HIS',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_IMSI',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_IMSI_USE',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_IMSI_HIS',
                                'default',
                                'rsclS',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('default',
                                'default',
                                'IM_INV_REINFORCE',
                                'priceCard',
                                'rsclR',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('default',
                                'default',
                                'IM_INV_REINFORCE_USE',
                                'priceCard',
                                'rsclR',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('default',
                                'default',
                                'IM_INV_REINFORCE_HIS',
                                'priceCard',
                                'rsclR',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_REALSIGN',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_REALSIGN_USE',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('sale',
                                'default',
                                'IM_INV_REALSIGN_HIS',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_REALSIGN',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_REALSIGN_USE',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'open',
                                'IM_INV_REALSIGN_HIS',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);

    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_REALSIGN',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_REALSIGN_USE',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);
    P_LOC_SALES_OUTLET_M_INSERT('present',
                                'noOpen',
                                'IM_INV_REALSIGN_HIS',
                                'default',
                                'rsclW',
                                v_region,
                                insysdate);

    COMMIT;
  END P_LOC_SALES_OUTLET_OUTLINE;


  PROCEDURE P_LOC_SALES_OUTLET_M_INSERT(v_sale_status  IN VARCHAR2,
                                        v_open         IN VARCHAR2,
                                        v_table        IN VARCHAR2,
                                        v_card_type    IN VARCHAR2,
                                        v_card_type_id IN VARCHAR2,
                                        v_region       IN NUMBER,
                                       insysdate       IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN

    v_sql := 'insert into LOC_SALES_OUTLET_MID(CREATE_TIME,SUBLIBRARY_CODE,MATERIAL_CODE,ERP_BATCH_ID,ACTIVITY_CODE,ACCOUNT_NAME,OUTLET_STAFF,region,OUID)
        select TO_CHAR(RE.RECDATE,''yyyymmdd'')  CREATE_TIME,
          (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(IMU.ORG_ID, 1, 14),
              ''SD.LE.01.01.s4'',
              ''SD.LE.01.01.s4'',
              decode(IMU.ORG_ID,
                     ''SD.LP.01.05'',
                     ''SD.LP.01.05'',
                     decode(substr(IMU.ORG_ID, 1, 14),
                            ''SD.LP.01.05.02'',
                            ''SD.LP.01.05.02'',
                            substr(IMU.ORG_ID,
                                   1,
                                   decode(instr(IMU.ORG_ID, ''.'', 1, 3),
                                          0,
                                          length(IMU.ORG_ID),
                                          instr(IMU.ORG_ID, ''.'', 1, 3) - 1)))))
              AND SE.FUNC_TYPE = ''SCM_SUBINV''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''CARDKIND''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''CARDKIND'') SUBLIBRARY_CODE,
          (SELECT ATT.ATTR_VALUE
             FROM IM_DICT.IM_RES_TYPE_ATTR_SET ATT
            WHERE ATT.ATTR_ID = ''ScmMaterialCode''
              AND IMU.RES_TYPE_ID = ATT.RES_TYPE_ID) MATERIAL_CODE,
          IMU.ERP_BATCH_ID,';

    IF v_card_type = 'priceCard' THEN
      v_sql := v_sql ||
               '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId3''
           AND id.groupid=''avtivityCodeScm'') ACTIVITY_CODE,';
    ELSE

      IF v_sale_status = 'sale' THEN
        v_sql := v_sql ||
                 '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId4''
           AND id.groupid=''avtivityCodeScm''AND REC.PRICE-REC.DISCOUNT>0) ACTIVITY_CODE,';
      ELSE

        IF v_sale_status = 'present' AND v_open = 'open' THEN
          v_sql := v_sql ||
                   '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId2''
           AND id.groupid=''avtivityCodeScm''
           AND REC.PRICE-REC.DISCOUNT=0 AND RE.recdefid IN (SELECT idi.itemid from im_dict_item idi
            WHERE idi.groupid=''openAccount'')) ACTIVITY_CODE, ';

        ELSE

          v_sql := v_sql ||
                   '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId1''
           AND id.groupid=''avtivityCodeScm''AND REC.PRICE-REC.DISCOUNT=0 AND
           RE.recdefid  not IN (SELECT idi.itemid from im_dict_item idi WHERE
            idi.groupid=''openAccount'')) ACTIVITY_CODE, ';
        END IF;
      END IF;
    END IF;

    IF v_card_type = 'priceCard' THEN
      v_sql := v_sql ||
               '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(IMU.ORG_ID,1,decode(instr(IMU.ORG_ID,''.'',1,3),0,length(IMU.ORG_ID),instr(IMU.ORG_ID,''.'',1,3)-1))
              AND IMU.RES_TYPE_ID = IRT.RES_TYPE_ID
              AND SE.FUNC_TYPE =  ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = IRT.RES_KIND_ID
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = IRT.RES_KIND_ID)  ACCOUNT_NAME,';

    ELSE
      IF v_sale_status = 'sale' THEN
        v_sql := v_sql ||
                 '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(IMU.ORG_ID,1,decode(instr(IMU.ORG_ID,''.'',1,3),0,length(IMU.ORG_ID),instr(IMU.ORG_ID,''.'',1,3)-1))
              AND IMU.RES_TYPE_ID = IRT.RES_TYPE_ID
              AND SE.FUNC_TYPE =  ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = IRT.RES_KIND_ID||''_SALE''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = IRT.RES_KIND_ID||''_SALE''
              AND REC.PRICE-REC.DISCOUNT>0)  ACCOUNT_NAME,';
      ELSE
        v_sql := v_sql ||
                 '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(IMU.ORG_ID,1,decode(instr(IMU.ORG_ID,''.'',1,3),0,length(IMU.ORG_ID),instr(IMU.ORG_ID,''.'',1,3)-1))
                  AND IMU.RES_TYPE_ID = IRT.RES_TYPE_ID
              AND SE.FUNC_TYPE = ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = IRT.RES_KIND_ID||''_PRESENT''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = IRT.RES_KIND_ID||''_PRESENT''
              AND REC.PRICE-REC.DISCOUNT=0)  ACCOUNT_NAME,';
      END IF;
    END IF;
    v_sql := v_sql || '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(IMU.ORG_ID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
              AND SE.FUNC_TYPE = ''SCM_OASTAFF''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OA''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OA'')  OUTLET_STAFF,
              RE.Region,
              (SELECT SE.DES_VALUE
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(IMU.ORG_ID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
              AND SE.FUNC_TYPE = ''SCM_OUID''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OUID''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OUID'') OUID
               FROM ' || v_table ||
             ' IMU,LOC_IM_REC_RESOURCE REC,LOC_IM_RECEPTION RE
    WHERE REC.RECOID = RE.OID
      AND RE.RECDATE BETWEEN TO_DATE('||insysdate||',''yyyymmdd'') AND TO_DATE('||insysdate||',''yyyymmdd'')+1
      AND REC.INOUT = ''0''
      AND RE.REGION='||v_region||'
      AND IMU.REGION='||v_region||'';
    IF v_card_type_id = 'rsclS' THEN
       IF v_sale_status = 'sale' THEN
          v_sql :=v_sql ||'AND REC.PRICE-REC.DISCOUNT>0';
       ELSE
          v_sql :=v_sql ||'AND REC.PRICE-REC.DISCOUNT=0';
          IF v_open = 'open' THEN
             v_sql :=v_sql ||'AND  RE.recdefid IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount'')';

          ELSE
             v_sql :=v_sql ||'AND  RE.recdefid  not IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount'')';
          END IF;
       END IF;
       v_sql := v_sql || 'AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                         WHERE IMU.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclS'')
                              AND IMU.Iccid = REC.BEGINRESID';
    ELSE
      IF v_card_type_id = 'rsclR' THEN
        v_sql := v_sql ||
                 'AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                         WHERE IMU.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclR'')
                              AND IMU.inv_id >= REC.BEGINRESID
                              AND IMU.inv_id<= REC.ENDRESID
                              AND IMU.inv_id LIKE substr(REC.BEGINRESID,1,10)||''%''
                              AND REC.restypeid LIKE ''rsclR%''';
      ELSE
        IF v_card_type_id = 'rsclW' THEN
           IF v_sale_status = 'sale' THEN
              v_sql :=v_sql ||'AND REC.PRICE-REC.DISCOUNT>0';
           ELSE
              v_sql :=v_sql ||'AND REC.PRICE-REC.DISCOUNT=0';
              IF v_open = 'open' THEN
                 v_sql :=v_sql ||'AND  RE.recdefid IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount'')';

              ELSE
                 v_sql :=v_sql ||'AND  RE.recdefid  not IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount'')';
          END IF;
       END IF;
        v_sql := v_sql ||
                 'AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                         WHERE IMU.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclW'')
                              AND IMU.inv_id = REC.BEGINRESID';
         END IF;
      END IF;
    END IF;

    EXECUTE IMMEDIATE v_sql;
    COMMIT;

 EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;

   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);

  END P_LOC_SALES_OUTLET_M_INSERT;
END P_LOC_SALES_OUTLET;
//
delimiter ;//
