delimiter /;
CREATE OR REPLACE PACKAGE "IM"."P_LOC_ALLOT_INLET" IS

  PROCEDURE P_LOC_ALLOT_INLET_OUTLINE(v_region  IN NUMBER,
                                      insysdate IN VARCHAR2);
  PROCEDURE P_LOC_ALLOT_INLET_M_INSERT(v_kind_type IN VARCHAR2,
                                       v_table1     IN VARCHAR2,
                                       v_region    IN NUMBER,
                                       insysdate   IN VARCHAR2,
                                       v_table2     IN VARCHAR2,
                                       v_org_id     IN VARCHAR2);
END P_LOC_ALLOT_INLET;
/

CREATE OR REPLACE PACKAGE BODY "IM"."P_LOC_ALLOT_INLET" IS
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
   --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细表,特殊渠道SD.LE.01.01.s4)
    P_LOC_ALLOT_INLET_M_INSERT('rsclS',
                               'IM_INOUT_STORE_DETAIL',
                               v_region,
                               insysdate,
                               'im_inv_imsi_use',
                               'SD.LE.01.01.s4');
   --sim卡调拨入库数据插入中间表(im_inv_imsi_use,出入库明细表,特殊渠道SD.LP.01.05.02)
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
    COMMIT;
  END P_LOC_ALLOT_INLET_OUTLINE;
  --SIM卡汇总数据到销售出库中间表
  PROCEDURE P_LOC_ALLOT_INLET_M_INSERT(v_kind_type IN VARCHAR2,
                                       v_table1     IN VARCHAR2,
                                       v_region    IN NUMBER,
                                       insysdate   IN VARCHAR2,
                                       v_table2     IN VARCHAR2,
                                       v_org_id     IN VARCHAR2) IS
    v_sql VARCHAR(4096);
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
  else
      v_hint2:=' ';
  end if;
    v_sql := 'insert into LOC_ALLOT_INLET_MID(CREATE_TIME,ORGANIZATION_CODE_OUT,ORGANIZATION_CODE_IN,ALLOT_TIME
    ,MATERIAL_CODE,ERP_BATCH_ID,ALLOT_STAFF,region,OUID)
        select
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
/

delimiter ;/



delimiter /;
CREATE OR REPLACE PACKAGE "IM"."P_LOC_BOSS_SCM_MIGRATION" IS

  PROCEDURE P_LOC_BOSS_SCM_OUTLINE(insysdate IN VARCHAR2);
  PROCEDURE P_LOC_SALES_OUTLET_INSERT(insysdate IN VARCHAR2);
  PROCEDURE P_LOC_ALLOT_INLET_INSERT(insysdate IN VARCHAR2);
  PROCEDURE P_LOC_SALES_CANCEL_INSERT(insysdate IN VARCHAR2);
  PROCEDURE P_LOC_SAVE_DATA;

END P_LOC_BOSS_SCM_MIGRATION;
/
CREATE OR REPLACE PACKAGE BODY "IM"."P_LOC_BOSS_SCM_MIGRATION" IS

  PROCEDURE P_LOC_BOSS_SCM_OUTLINE(insysdate IN VARCHAR2) IS
  BEGIN
    P_LOC_SALES_OUTLET_INSERT(insysdate);
    P_LOC_ALLOT_INLET_INSERT(insysdate);
    P_LOC_SALES_CANCEL_INSERT(insysdate);
    P_LOC_SAVE_DATA;
    COMMIT;
  END P_LOC_BOSS_SCM_OUTLINE;
  PROCEDURE P_LOC_SALES_OUTLET_INSERT(insysdate IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN
  DELETE FROM LOC_SALES_OUTLET t
     WHERE t.CREATE_TIME = insysdate;

    v_sql := 'INSERT INTO LOC_SALES_OUTLET(CREATE_TIME,SUBLIBRARY_CODE,MATERIAL_CODE,SALE_NUMBER,ERP_BATCH_ID,ACTIVITY_CODE,ACCOUNT_NAME,OUTLET_STAFF,OUID)
            SELECT lo.create_time,lo.sublibrary_code,lo.material_code,count(1),lo.erp_batch_id,lo.activity_code,lo.account_name,lo.outlet_staff,lo.ouid
  from loc_sales_outlet_mid lo WHERE lo.create_time='||insysdate||' AND lo.activity_code IS NOT NULL AND lo.account_name IS NOT NULL GROUP BY lo.create_time,lo.sublibrary_code,lo.material_code,lo.erp_batch_id,lo.activity_code,lo.account_name,lo.outlet_staff,lo.ouid';

    EXECUTE IMMEDIATE v_sql;
    COMMIT;
     EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;
   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
  END P_LOC_SALES_OUTLET_INSERT;
  PROCEDURE P_LOC_ALLOT_INLET_INSERT(insysdate IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN
   DELETE FROM LOC_ALLOT_INLET t
     WHERE t.CREATE_TIME = insysdate;
    v_sql := 'INSERT INTO LOC_ALLOT_INLET(CREATE_TIME,ORGANIZATION_CODE_OUT,ORGANIZATION_CODE_IN,ALLOT_TIME
    ,MATERIAL_CODE,ALLOT_NUMBER,ERP_BATCH_ID,ALLOT_STAFF,OUID)
            SELECT lo.CREATE_TIME,lo.ORGANIZATION_CODE_OUT,lo.ORGANIZATION_CODE_IN,lo.ALLOT_TIME,lo.MATERIAL_CODE,count(1),lo.ERP_BATCH_ID,lo.ALLOT_STAFF,lo.OUID
  from LOC_ALLOT_INLET_MID lo WHERE lo.create_time='||insysdate||' GROUP BY lo.CREATE_TIME,lo.ORGANIZATION_CODE_OUT,lo.ORGANIZATION_CODE_IN,lo.ALLOT_TIME,lo.MATERIAL_CODE,lo.ALLOT_NUMBER,lo.ERP_BATCH_ID,lo.ALLOT_STAFF,lo.OUID';
    EXECUTE IMMEDIATE v_sql;
    COMMIT;
     EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;
   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
  END P_LOC_ALLOT_INLET_INSERT;
  PROCEDURE P_LOC_SALES_CANCEL_INSERT(insysdate IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN
    DELETE FROM LOC_SALES_CANCEL_STOCKS t
     WHERE t.CREATE_TIME = insysdate;
    v_sql := 'INSERT INTO LOC_SALES_CANCEL_STOCKS(CREATE_TIME,SUBLIBRARY_CODE,MATERIAL_CODE,OUTLET_NUMBER,OUTLET_CODE,ERP_BATCH_ID,ACTIVITY_CODE,ACCOUNT_NAME,OUTLET_STAFF,OUID)
            SELECT lo.create_time,lo.sublibrary_code,lo.material_code,count(1),lo.outlet_code,lo.erp_batch_id,lo.activity_code,lo.account_name,lo.outlet_staff,lo.ouid
  from LOC_SALES_CANCEL_STOCKS_MID lo WHERE lo.create_time='||insysdate||' AND lo.activity_code IS NOT NULL AND lo.account_name IS NOT NULL GROUP BY lo.create_time,lo.sublibrary_code,lo.material_code,lo.outlet_code,lo.erp_batch_id,lo.activity_code,lo.account_name,lo.outlet_staff,lo.ouid';
    EXECUTE IMMEDIATE v_sql;
    COMMIT;
     EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;
   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
  END P_LOC_SALES_CANCEL_INSERT;
  PROCEDURE P_LOC_SAVE_DATA IS
  BEGIN
   DELETE FROM loc_sales_outlet_mid ls WHERE to_date(ls.create_time,'yyyymmdd')<trunc(SYSDATE,'dd')-7;
   DELETE FROM LOC_SALES_CANCEL_STOCKS_MID ls WHERE to_date(ls.create_time,'yyyymmdd')<trunc(SYSDATE,'dd')-7;
   DELETE FROM LOC_ALLOT_INLET_MID ls WHERE to_date(ls.create_time,'yyyymmdd')<trunc(SYSDATE,'dd')-7;

   DELETE FROM loc_sales_outlet ls WHERE to_date(ls.create_time,'yyyymmdd')<add_months(trunc(SYSDATE,'mm'),-3);
   DELETE FROM LOC_SALES_CANCEL_STOCKS ls WHERE to_date(ls.create_time,'yyyymmdd')<add_months(trunc(SYSDATE,'mm'),-3);
   DELETE FROM LOC_ALLOT_INLET ls WHERE to_date(ls.create_time,'yyyymmdd')<add_months(trunc(SYSDATE,'mm'),-3);
     COMMIT;
      EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;
   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
  END P_LOC_SAVE_DATA;
END P_LOC_BOSS_SCM_MIGRATION;
/

delimiter ;/



delimiter /;
CREATE OR REPLACE PACKAGE "IM"."P_LOC_SALES_CANCEL_STOCKS" IS

  PROCEDURE P_LOC_SALES_CANCEL_OUTLINE(v_region  IN NUMBER,
                                       insysdate IN VARCHAR2);
  PROCEDURE P_LOC_SALES_CANCEL_M_INSERT(v_sale_status  IN VARCHAR2,
                                        v_open         IN VARCHAR2,
                                        v_table        IN VARCHAR2,
                                        v_region       IN NUMBER,
                                       insysdate       IN VARCHAR2);
END P_LOC_SALES_CANCEL_STOCKS;

/
CREATE OR REPLACE PACKAGE BODY "IM"."P_LOC_SALES_CANCEL_STOCKS" IS
  PROCEDURE P_LOC_SALES_CANCEL_OUTLINE(v_region  IN NUMBER,
                                       insysdate IN VARCHAR2) IS
  BEGIN
    --根据时间和地市清除中间表数据
    DELETE FROM LOC_SALES_CANCEL_STOCKS_MID t
     WHERE t.CREATE_TIME = insysdate AND t.region=v_region;

    --1.sim卡数据插入销售退库中间表
    --销售
    P_LOC_SALES_CANCEL_M_INSERT('sale', 'default', 'IM_INV_IMSI',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('sale', 'default', 'IM_INV_IMSI_USE',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('sale', 'default', 'IM_INV_IMSI_HIS',v_region,insysdate);
    --赠送开户
    P_LOC_SALES_CANCEL_M_INSERT('present', 'open', 'IM_INV_IMSI',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('present', 'open', 'IM_INV_IMSI_USE',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('present', 'open', 'IM_INV_IMSI_HIS',v_region,insysdate);
    --赠送非开户
    P_LOC_SALES_CANCEL_M_INSERT('present', 'noOpen', 'IM_INV_IMSI',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('present', 'noOpen', 'IM_INV_IMSI_USE',v_region,insysdate);
    P_LOC_SALES_CANCEL_M_INSERT('present', 'noOpen', 'IM_INV_IMSI_HIS',v_region,insysdate);
    COMMIT;
  END P_LOC_SALES_CANCEL_OUTLINE;
  --SIM卡汇总数据到销售退库中间表
  PROCEDURE P_LOC_SALES_CANCEL_M_INSERT(v_sale_status IN VARCHAR2,
                                        v_open        IN VARCHAR2,
                                        v_table       IN VARCHAR2,
                                        v_region      IN NUMBER,
                                        insysdate     IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN
    v_sql := 'insert into LOC_SALES_CANCEL_STOCKS_MID(CREATE_TIME,SUBLIBRARY_CODE,MATERIAL_CODE,ERP_BATCH_ID,ACTIVITY_CODE,
        ACCOUNT_NAME,OUTLET_STAFF,REGION,OUID)
        select TO_CHAR(RE.RECDATE,''yyyymmdd'') CREATE_TIME,
          (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
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
     --业务活动编码取值，SIM卡销售
      IF v_sale_status = 'sale' THEN
        v_sql := v_sql || '(SELECT id.itemname FROM im_dict_item id
         WHERE id.itemid=''avtivityCodeScmId4''
           AND id.groupid=''avtivityCodeScm''AND REC.PRICE-REC.DISCOUNT>0) ACTIVITY_CODE,';
      ELSE
       --业务活动编码取值，赠送开户
        IF v_sale_status = 'present' AND v_open = 'open' THEN
          v_sql := v_sql || '(SELECT id.itemname FROM im_dict_item id
          WHERE id.itemid=''avtivityCodeScmId2''
           AND id.groupid=''avtivityCodeScm''
           AND REC.PRICE-REC.DISCOUNT=0
           AND RE.recdefid IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount'')) ACTIVITY_CODE, ';

        ELSE
        --业务活动编码取值，赠送非开户
          v_sql := v_sql || '(SELECT id.itemname FROM im_dict_item id
          WHERE id.itemid=''avtivityCodeScmId1''
                 AND id.groupid=''avtivityCodeScm''
                 AND REC.PRICE-REC.DISCOUNT=0
                 AND RE.recdefid not IN (SELECT idi.itemid from im_dict_item idi WHERE idi.groupid=''openAccount''))ACTIVITY_CODE, ';
        END IF;
      END IF;
    --账户别名取值，销售
    IF v_sale_status = 'sale' THEN
      v_sql := v_sql ||
               '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
              AND SE.FUNC_TYPE =  ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''rsclS_SALE''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''rsclS_SALE''
              AND REC.PRICE-REC.DISCOUNT>0)  ACCOUNT_NAME,';
    ELSE
    --账户别名取值，赠送
      v_sql := v_sql ||
               '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
              AND SE.FUNC_TYPE = ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''rsclS_PRESENT''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''rsclS_PRESENT''
              AND REC.PRICE-REC.DISCOUNT=0)  ACCOUNT_NAME,';
    END IF;
    v_sql := v_sql || '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(RE.RECORGID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
              AND SE.FUNC_TYPE = ''SCM_OASTAFF''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OA''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OA'')  OUTLET_STAFF,
              RE.Region,
              (SELECT SE.DES_VALUE
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(RE.RECORGID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
              AND SE.FUNC_TYPE = ''SCM_OUID''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OUID''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OUID'') OUID
               FROM ' || v_table ||
             ' IMU,LOC_IM_RECEPTION RE,LOC_IM_REC_RESOURCE REC
    WHERE  REC.RECOID = RE.OID
      AND RE.RECDEFID=''AgentBackCard''
      AND RE.RECDATE BETWEEN TO_DATE('||insysdate||',''yyyymmdd'') AND TO_DATE('||insysdate||',''yyyymmdd'')+1
      AND RE.REGION='||v_region||'
      AND REC.REGION='||v_region||'
      AND IMU.REGION='||v_region||'
      AND EXISTS (SELECT '''' FROM IM_DICT_ITEM DI
                        WHERE IMU.RES_TYPE_ID = DI.ITEMID
                              AND DI.GROUPID=''BossSupplyChainRsclS'')
    AND IMU.Iccid = REC.BEGINRESID
    AND REC.restypeid LIKE ''rsclS%''';

    EXECUTE IMMEDIATE v_sql;
    COMMIT;
     EXCEPTION WHEN no_data_found THEN
   dbms_output.put_line('-->Error:No data found');
   dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);
   RETURN;
   WHEN others THEN
     dbms_output.put_line('-->SQLCODE:'||SQLCODE|| ' ' || SQLERRM);

  END P_LOC_SALES_CANCEL_M_INSERT;
END P_LOC_SALES_CANCEL_STOCKS;
/

delimiter ;/



delimiter /;
CREATE OR REPLACE PACKAGE "IM"."P_LOC_SALES_OUTLET" IS
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
/

delimiter ;/


delimiter /;
CREATE OR REPLACE PACKAGE BODY "IM"."P_LOC_SALES_OUTLET" IS
  PROCEDURE P_LOC_SALES_OUTLET_OUTLINE(v_region  IN NUMBER,
                                       insysdate IN VARCHAR2) IS
  BEGIN
    --根据时间和地市清除中间表数据
    DELETE FROM LOC_SALES_OUTLET_MID t
     WHERE t.CREATE_TIME = insysdate AND t.region=v_region;
    --1.sim卡数据插入销售出库中间表
    --销售
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
    --赠送开户
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
    --赠送非开户
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
    --2.有价卡数据插入销售出库中间表
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
    --3.空白卡数据插入出库中间表
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
    --赠送开户
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
    --赠送非开户
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

  --SIM卡汇总数据到销售出库中间表
  PROCEDURE P_LOC_SALES_OUTLET_M_INSERT(v_sale_status  IN VARCHAR2,
                                        v_open         IN VARCHAR2,
                                        v_table        IN VARCHAR2,
                                        v_card_type    IN VARCHAR2,
                                        v_card_type_id IN VARCHAR2,
                                        v_region       IN NUMBER,
                                       insysdate       IN VARCHAR2) IS
    v_sql VARCHAR(4096);
  BEGIN
  --ACTIVITY_CODE,ACCOUNT_NAME,
    v_sql := 'insert into LOC_SALES_OUTLET_MID(CREATE_TIME,SUBLIBRARY_CODE,MATERIAL_CODE,ERP_BATCH_ID,ACTIVITY_CODE,ACCOUNT_NAME,OUTLET_STAFF,region,OUID)
        select TO_CHAR(RE.RECDATE,''yyyymmdd'')  CREATE_TIME,
          (SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(RE.RECORGID, 1, 14),
              ''SD.LE.01.01.s4'',
              ''SD.LE.01.01.s4'',
              decode(RE.RECORGID,
                     ''SD.LP.01.05'',
                     ''SD.LP.01.05'',
                     decode(substr(RE.RECORGID, 1, 14),
                            ''SD.LP.01.05.02'',
                            ''SD.LP.01.05.02'',
                            substr(RE.RECORGID,
                                   1,
                                   decode(instr(RE.RECORGID, ''.'', 1, 3),
                                          0,
                                          length(RE.RECORGID),
                                          instr(RE.RECORGID, ''.'', 1, 3) - 1)))))
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
    --业务活动编码取值，有价卡
    IF v_card_type = 'priceCard' THEN
      v_sql := v_sql ||
               '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId3''
           AND id.groupid=''avtivityCodeScm'') ACTIVITY_CODE,';
    ELSE
      --业务活动编码取值，SIM卡销售
      IF v_sale_status = 'sale' THEN
        v_sql := v_sql ||
                 '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId4''
           AND id.groupid=''avtivityCodeScm''AND REC.PRICE-REC.DISCOUNT>0) ACTIVITY_CODE,';
      ELSE
        --业务活动编码取值，赠送开户
        IF v_sale_status = 'present' AND v_open = 'open' THEN
          v_sql := v_sql ||
                   '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId2''
           AND id.groupid=''avtivityCodeScm''
           AND REC.PRICE-REC.DISCOUNT=0 AND RE.recdefid IN (SELECT idi.itemid from im_dict_item idi
            WHERE idi.groupid=''openAccount'')) ACTIVITY_CODE, ';

        ELSE
          --业务活动编码取值，赠送非开户
          v_sql := v_sql ||
                   '(SELECT id.itemname FROM im_dict_item id WHERE id.itemid=''avtivityCodeScmId1''
           AND id.groupid=''avtivityCodeScm''AND REC.PRICE-REC.DISCOUNT=0 AND
           RE.recdefid  not IN (SELECT idi.itemid from im_dict_item idi WHERE
            idi.groupid=''openAccount'')) ACTIVITY_CODE, ';
        END IF;
      END IF;
    END IF;
   --账户名取值，有价卡
    IF v_card_type = 'priceCard' THEN
      v_sql := v_sql ||
               '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
              AND IMU.RES_TYPE_ID = IRT.RES_TYPE_ID
              AND SE.FUNC_TYPE =  ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = IRT.RES_KIND_ID
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = IRT.RES_KIND_ID)  ACCOUNT_NAME,';
      --账户别名取值，sim卡销售
    ELSE
      IF v_sale_status = 'sale' THEN
        v_sql := v_sql ||
                 '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
              AND IMU.RES_TYPE_ID = IRT.RES_TYPE_ID
              AND SE.FUNC_TYPE =  ''SCM_ALIASNAME''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = IRT.RES_KIND_ID||''_SALE''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = IRT.RES_KIND_ID||''_SALE''
              AND REC.PRICE-REC.DISCOUNT>0)  ACCOUNT_NAME,';
      ELSE
        --账户别名取值，sim卡赠送
        v_sql := v_sql ||
                 '(SELECT SE.NOTES
             FROM IM_SYSTEM_EXCH SE,IM_RES_TYPE IRT
            WHERE SE.SRC_VALUE = substr(RE.RECORGID,1,decode(instr(RE.RECORGID,''.'',1,3),0,length(RE.RECORGID),instr(RE.RECORGID,''.'',1,3)-1))
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
            WHERE SE.SRC_VALUE = decode(substr(RE.RECORGID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
              AND SE.FUNC_TYPE = ''SCM_OASTAFF''
              AND SE.SRC_SYSTEM = ''CRM''
              AND SE.SRC_TYPE = ''OA''
              AND SE.DES_SYSTEM = ''SCM''
              AND SE.DES_TYPE = ''OA'')  OUTLET_STAFF,
              RE.Region,
              (SELECT SE.DES_VALUE
             FROM IM_SYSTEM_EXCH SE
            WHERE SE.SRC_VALUE = decode(substr(RE.RECORGID,1,5),''SD.LP'',''634'',''SD.LA'',''531'',RE.Region)
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
    --取值数据所属资源类型
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
/

delimiter ;/


delimiter /;
CREATE OR REPLACE PACKAGE "IM"."PKG_LOC_STAT_APPLE" is
  procedure P_loc_stat_apple(p_region number);
  procedure p_loc_stat_apple_main(i_region        IN number,
                                  i_orgid         IN varchar2,
                                  i_res_typeid    IN varchar2,
                                  i_materiel_code IN varchar2,
                                  i_BUSI_STATUS   IN varchar2,
                                  o_teml_count         OUT number,
                                  o_inQuantity         OUT number,
                                  o_loadInQuantity     OUT number,
                                  o_loadInQuantity_his OUT number,
                                  o_saleRollQuantity   OUT number,
                                  o_createRollQuantity OUT number,
                                  o_loadOutQuantity    OUT number,
                                  o_saleQuantity       OUT number);

  procedure p_loc_stat_apple_saled(i_region                 IN number,
                                   i_orgid                  IN varchar2,
                                   i_res_typeid             IN varchar2,
                                   o_saleQuantity           OUT number,
                                   o_saleQuantity_added     OUT number,
                                   o_saleQuantity_decrement OUT number);

end PKG_LOC_STAT_APPLE;
/

CREATE OR REPLACE PACKAGE BODY "IM"."PKG_LOC_STAT_APPLE" is
  procedure P_loc_stat_apple(p_region number) is
    v_region varchar2(8);
    v_teml_count        number := 0;
    v_teml_count_sample number := 0;
    --当日可售库存增加数量=当日单位的入库数量+当日单位调拨入库数量+当日单位销售回退数量
    v_cur_addAuantity        number := 0;
    v_cur_addAuantity_sample number := 0;
    ----2.1 当日单位入库数量
    v_inQuantity number := 0;
    ----2.2 当日单位调拨入库数量
    v_loadInQuantity number := 0;
    ----2.2 当日单位调拨入库数量his表
    v_loadInQuantity_his number := 0;
    ----2.3 销售回退数量
    v_saleRollQuantity number := 0;
    --当日可售库存减少数量=当日单位的生成回退数量+当日单位调拨出库数量+当日单位销售数量
    v_cur_decrement        number := 0;
    v_cur_decrement_sample number := 0;
    --当日单位的生成回退数量
    v_createRollQuantity number := 0;
    --当日单位调拨出库数量
    v_loadOutQuantity number := 0;
    --当日单位销售数量
    v_saleQuantity number := 0;
    v_storetype    varchar2(1); --库存点类型,1——零售网点,2——供货渠道仓库
    v_shopcode     varchar2(16);
    v_memo         varchar2(256);
    --自营渠道的仓库编码
    v_warehousecode_self varchar2(16);
    --社会渠道的仓库编码
    v_warehousecode_other varchar2(16);
    v_count               number := 0;
    v_orgid               varchar2(32);
    v_materiel_code       varchar2(16);
    --v_res_typeid          varchar2(32);
    v_saleQuantity_sum       number := 0;
    v_saleQuantity_added     number := 0;
    v_saleQuantity_decrement number := 0;
  begin
    v_region := p_region;
    delete LOC_IM_IF_BB_STOCK where region = v_region;
    commit;
    select orgid into v_orgid from region_list where region = v_region;
    --自营渠道取WAREHOUSECODE   根据region查询region_list表中ORGID
    begin
      select WAREHOUSECODE
        into v_warehousecode_self
        from IM_CFG_MOBSUPPCHANNEL t
       where t.store_id = v_orgid;
    exception
      when others then
        return;
    end;
    --社会渠道取WAREHOUSECODE
    begin
      select WAREHOUSECODE
        into v_warehousecode_other
        from IM_CFG_MOBSUPPCHANNEL t
       where t.store_id = v_orgid || '2';
    exception
      when others then
        return;
    end;
    for v_record1 in ( --查出所有有库存的单位
                      select distinct (org_id) orgid
                        from im_inv_mobtel t
                       where t.region = v_region
                         and t.res_type_id in
                             (select RES_TYPE_ID from loc_stat_apple_res_type)) loop
      --ORG_ID在im_cfg_mobileshop表中为  零售网点
      v_shopcode := '';
      begin
        select t.shopcode
          into v_shopcode
          from im_cfg_mobileshop t
         where t.region = v_region
           and t.orgid = v_record1.orgid;
      exception
        when others then
          v_shopcode := '';
      end;
      if v_shopcode is null or v_shopcode = '' then
        --ORG_ID在IM_CFG_MOBSUPPCHANNEL表中为  供货渠道
        --供货渠道   分为自营和社会渠道两种
        --判断条件为
        --查出结果为自营渠道，查不到为社会渠道
        SELECT count(1)
          into v_count
          FROM AGENT t
         WHERE t.region = v_region
           AND t.agentid = v_record1.orgid
           AND t.agenttype IN ('110101', '110102', '110201');
        if v_count > 0 then
          v_shopcode := v_warehousecode_self;
        else
          v_shopcode := v_warehousecode_other;
          v_orgid    := v_orgid || '2';
        end if;
        v_storetype := '2';
        v_memo      := '供货渠道仓库当日的终端库存量';
      else
        v_memo      := '零售网点当日的终端库存量';
        v_storetype := '1';
        v_orgid     := v_record1.orgid;
      end if;
      for v_record2 in (select distinct (RES_TYPE_ID) RESTYPEID
                          from im_inv_mobtel t
                         where t.region = v_region
                           and t.org_id = v_record1.orgid
                           and t.res_type_id in
                               (select RES_TYPE_ID
                                  from loc_stat_apple_res_type)) loop
        begin
          select b.materiel_code
            into v_materiel_code
            from IM_SYSTEM_EXCH a, im_if_bb_productinfo b
           where a.FUNC_TYPE = 'GRPMOBTEL'
             and a.SRC_TYPE = 'RES_TYPE'
             and b.product_id = a.src_value
             and a.des_value = v_record2.restypeid
             and rownum < 2;
        exception
          when others then
            v_materiel_code := '';
            goto LOOPCONTINUE;
        end;
        p_loc_stat_apple_main(v_region,
                              v_record1.orgid,
                              v_record2.restypeid,
                              v_materiel_code,
                              'USABLE',                             
                              v_teml_count,
                              v_inQuantity,
                              v_loadInQuantity,
                              v_loadInQuantity_his,
                              v_saleRollQuantity,
                              v_createRollQuantity,
                              v_loadOutQuantity,
                              v_saleQuantity);
        --if v_teml_count > 0 then
        --当日可售库存增加数量=当日单位的入库数量+当日单位调拨入库数量+当日单位销售回退数量
        v_cur_addAuantity :=  ----2.1 当日单位入库数量
         v_inQuantity +
                            ----2.2 当日单位调拨入库数量
                             v_loadInQuantity +
                            ----2.2 当日单位调拨入库数量his表
                             v_loadInQuantity_his +
                            ----2.3 销售回退数量
                             v_saleRollQuantity;
        --当日可售库存减少数量=当日单位的生成回退数量+当日单位调拨出库数量+当日单位销售数量
        v_cur_decrement :=  --当日单位的生成回退数量
         v_createRollQuantity +
                          --当日单位调拨出库数量
                           v_loadOutQuantity +
                          --当日单位销售数量
                           v_saleQuantity;
        p_loc_stat_apple_saled(v_region,
                               v_record1.orgid,
                               v_record2.restypeid,
                               v_saleQuantity_sum,
                               v_saleQuantity_added,
                               v_saleQuantity_decrement);
        insert into LOC_IM_IF_BB_STOCK
          (REGION,
           INTF_DATE,
           REC_SEQ,
           RES_TYPE_ID,
           MATERIALCODE,
           ORGID,
           SHOPCODE,
           STATQUANITY,
           STATUS,
           STATUS_DATE,
           MEMO,
           CENTER_STATUS,
           --add
           STORETYPE, --库存点类型,1——零售网点,2——供货渠道仓库
           IN_SELLABLE, --当日可售库存增加数量
           OUT_SELLABLE, --当日可售库存减少数量
           QUANTITY_RESERVED, --终端已售库存量
           IN_DEMO, --当日样机库存增加数量
           OUT_DEMO, --当日样机库存减少数量
           IN_BACKORDER, --当日预售库存增加数量
           IN_RESERVED, --当日已售库存增加数量
           QUANTITY_UNSELLABLE, --终端不可售库存量
           IN_UNSELLABLE, --当日不可售库存增加数量
           OUT_BACKORDER, --当日预售库存减少数量
           OUT_UNSELLABLE, --当日不可售库存减少数量
           OUT_RESERVED, --当日已售库存减少数量
           QUANTITY_DEMO, --终端样机库存量
           QUANTITY_BACKORDER --终端预售库存量
           )
        values
          (v_region,
           to_char(sysdate, 'yyyymmdd'),
           0,
           v_record2.restypeid, --资源类型编码
           v_materiel_code, --物料编码
           v_orgid, --组织单位
           v_shopcode, --零售网点统一编码
           v_teml_count, --库存量
           'PEND', --待处理
           sysdate,
           v_memo, --备注
           'PEND', --一级boss确认状态 插入为:PEND
           --add
           v_storetype, --库存点类型,1——零售网点,2——供货渠道仓库
           v_cur_addAuantity, --当日可售库存增加数量
           v_cur_decrement, --当日可售库存减少数量
           v_saleQuantity_sum, --终端已售库存量
           v_cur_addAuantity_sample, --当日样机库存增加数量
           v_cur_decrement_sample, --当日样机库存减少数量
           0, --当日预售库存增加数量
           v_saleQuantity_added, --当日已售库存增加数量
           0, --终端不可售库存量
           0, --当日不可售库存增加数量
           0, --当日预售库存减少数量
           0, --当日不可售库存减少数量
           v_saleQuantity_decrement, --当日已售库存减少数量
           v_teml_count_sample, --终端样机库存量
           0 --终端预售库存量
           );
        commit;
        <<LOOPCONTINUE>>
        null;
      end loop;
    end loop;
    insert into IM_IF_BB_STOCK
      (REGION,
       INTF_DATE,
       REC_SEQ,
       RES_TYPE_ID,
       MATERIALCODE,
       ORGID,
       SHOPCODE,
       STATQUANITY,
       STATUS,
       STATUS_DATE,
       MEMO,
       CENTER_STATUS,
       --add
       STORETYPE, --库存点类型,1——零售网点,2——供货渠道仓库
       IN_SELLABLE, --当日可售库存增加数量
       OUT_SELLABLE, --当日可售库存减少数量
       QUANTITY_RESERVED, --终端已售库存量
       IN_DEMO, --当日样机库存增加数量
       OUT_DEMO, --当日样机库存减少数量
       IN_BACKORDER, --当日预售库存增加数量
       IN_RESERVED, --当日已售库存增加数量
       QUANTITY_UNSELLABLE, --终端不可售库存量
       IN_UNSELLABLE, --当日不可售库存增加数量
       OUT_BACKORDER, --当日预售库存减少数量
       OUT_UNSELLABLE, --当日不可售库存减少数量
       OUT_RESERVED, --当日已售库存减少数量
       QUANTITY_DEMO, --终端样机库存量
       QUANTITY_BACKORDER --终端预售库存量
       )
      select REGION,
             INTF_DATE,
             to_char(sysdate, 'yymmdd') ||
             substr(to_char(SEQ_BUSI_ID.nextval), -8),
             RES_TYPE_ID,
             MATERIALCODE,
             ORGID,
             SHOPCODE,
             STATQUANITY,
             STATUS,
             sysdate,
             MEMO,
             CENTER_STATUS,
             --add
             STORETYPE, --库存点类型,1——零售网点,2——供货渠道仓库
             IN_SELLABLE, --当日可售库存增加数量
             OUT_SELLABLE, --当日可售库存减少数量
             QUANTITY_RESERVED, --终端已售库存量
             IN_DEMO, --当日样机库存增加数量
             OUT_DEMO, --当日样机库存减少数量
             IN_BACKORDER, --当日预售库存增加数量
             IN_RESERVED, --当日已售库存增加数量
             QUANTITY_UNSELLABLE, --终端不可售库存量
             IN_UNSELLABLE, --当日不可售库存增加数量
             OUT_BACKORDER, --当日预售库存减少数量
             OUT_UNSELLABLE, --当日不可售库存减少数量
             OUT_RESERVED, --当日已售库存减少数量
             QUANTITY_DEMO, --终端样机库存量
             QUANTITY_BACKORDER --终端预售库存量
        from (select REGION,
                     to_char(sysdate, 'yyyymmdd') INTF_DATE,
                    RES_TYPE_ID,
                     MATERIALCODE,
                     ORGID,
                     SHOPCODE,
                     sum(STATQUANITY) STATQUANITY,
                     'PEND' STATUS,
                     MEMO,
                     'PEND' CENTER_STATUS,
                     --add
                     STORETYPE, --库存点类型,1——零售网点,2——供货渠道仓库
                     sum(IN_SELLABLE) IN_SELLABLE, --当日可售库存增加数量
                     sum(OUT_SELLABLE) OUT_SELLABLE, --当日可售库存减少数量
                     sum(QUANTITY_RESERVED) QUANTITY_RESERVED, --终端已售库存量
                     sum(IN_DEMO) IN_DEMO, --当日样机库存增加数量
                     sum(OUT_DEMO) OUT_DEMO, --当日样机库存减少数量
                     sum(IN_BACKORDER) IN_BACKORDER, --当日预售库存增加数量
                     sum(IN_RESERVED) IN_RESERVED, --当日已售库存增加数量
                     sum(QUANTITY_UNSELLABLE) QUANTITY_UNSELLABLE, --终端不可售库存量
                     sum(IN_UNSELLABLE) IN_UNSELLABLE, --当日不可售库存增加数量
                     sum(OUT_BACKORDER) OUT_BACKORDER, --当日预售库存减少数量
                     sum(OUT_UNSELLABLE) OUT_UNSELLABLE, --当日不可售库存减少数量
                     sum(OUT_RESERVED) OUT_RESERVED, --当日已售库存减少数量
                     sum(QUANTITY_DEMO) QUANTITY_DEMO, --终端样机库存量
                     sum(QUANTITY_BACKORDER) QUANTITY_BACKORDER --终端预售库存量
                from LOC_IM_IF_BB_STOCK
               where region = v_region
               group by region,
                        RES_TYPE_ID,
                        materialcode,
                        orgid,
                        shopcode,
                        memo,
                        STORETYPE) a;
    commit;
  end P_loc_stat_apple;
  procedure p_loc_stat_apple_main(i_region        IN number,
                                  i_orgid         IN varchar2,
                                  i_res_typeid    IN varchar2,
                                  i_materiel_code IN varchar2,
                                  i_BUSI_STATUS   IN varchar2,
                                  o_teml_count         OUT number,
                                  o_inQuantity         OUT number,
                                  o_loadInQuantity     OUT number,
                                  o_loadInQuantity_his OUT number,
                                  o_saleRollQuantity   OUT number,
                                  o_createRollQuantity OUT number,
                                  o_loadOutQuantity    OUT number,
                                  o_saleQuantity       OUT number) is
  begin
    select count(*) TEML_COUNT
      into o_teml_count
      from IM_INV_MOBTEL
     where region = i_region
       and ORG_ID = i_orgid
       and RES_TYPE_ID = i_res_typeid
       and INV_STATUS = 'INSTORE'
       and BUSI_STATUS = i_BUSI_STATUS;
    --逐个计算
    --2、当日可售库存增加数量=当日单位的入库数量+当日单位调拨入库数量+当日单位销售回退数量
    ----2.1 当日单位入库数量
    select
     count(1) cnt
      into o_inQuantity
      from im_ldstore_batch_detail a, im_if_imei_info b
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.store_id in
           (select c.store_id from im_store c where c.org_id = i_orgid)
       and a.status_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.inv_id = b.inv_id
       and a.res_type_id = i_res_typeid
       and a.inv_id = b.inv_id
       and a.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and b.materialcode = i_materiel_code;
    ----2.2 当日单位调拨入库数量
    select
     count(1)
      into o_loadInQuantity
      from im_busi_opera a, im_inout_store_detail b, im_if_imei_info c
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.busi_type in ('ASIGIN', 'ASIGINDIFFREGION')
       and a.begin_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.busi_id = b.busi_id
       and b.inv_id = c.inv_id
       and b.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and a.status = 'CLOSED'
       and c.materialcode = i_materiel_code;

    select
     count(1)
      into o_loadInQuantity_his
      from im_busi_opera a, Im_Inout_Store_Detail_His b, im_if_imei_info c
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.busi_type in ('ASIGIN', 'ASIGINDIFFREGION')
       and a.begin_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.busi_id = b.busi_id
       and b.inv_id = c.inv_id
       and b.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and a.status = 'CLOSED'
       and c.materialcode = i_materiel_code;

    ----2.3 销售回退数量
    select
     count(1)
      into o_saleRollQuantity
      from im_busi_opera a, IM_SALE_DETAIL b, im_if_imei_info c
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.busi_type in ('UNSALE', 'SALE_ROLLBACK')
       and a.begin_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.busi_id = b.busi_id
       and b.inv_id = c.inv_id
       and b.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and c.materialcode = i_materiel_code;

    --3、当日可售库存减少数量=当日单位的生成回退数量+当日单位调拨出库数量+当日单位销售数量
    --当日单位的生成回退数量
    select
     count(1) cnt
      into o_createRollQuantity
      from im_rollback_detail a, im_if_imei_info b
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.store_id in
           (select c.store_id from im_store c where c.org_id = i_orgid)
       and a.status_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.inv_id = b.inv_id
       and a.res_type_id = i_res_typeid
       and a.inv_id = b.inv_id
       and a.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and b.materialcode = i_materiel_code;
    --当日单位调拨出库数量
    select
     count(1)
      into o_loadOutQuantity
      from im_busi_opera a, im_inout_store_detail_his b, im_if_imei_info c
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.busi_type in ('ASIGOUT')
       and a.begin_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.busi_id = b.busi_id
       and b.inv_id = c.inv_id
       and b.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and c.materialcode = i_materiel_code;
    --当日单位销售数量
    select
     count(1)
      into o_saleQuantity
      from im_busi_opera a, IM_SALE_DETAIL b, im_if_imei_info c
     where a.res_kind_id = 'rsclM'
       and a.region = i_region
       and a.busi_type in ('SALE')
       and a.begin_date > sysdate - 1
       and a.busi_id > to_char(sysdate - 1, 'YYMMDD')
       and a.busi_id = b.busi_id
       and b.inv_id = c.inv_id
       and b.inv_id in (select inv_id
                          from IM_INV_MOBTEL
                         where region = i_region
                           and ORG_ID = i_orgid
                           and RES_TYPE_ID = i_res_typeid
                           and INV_STATUS = 'INSTORE'
                           and BUSI_STATUS = i_BUSI_STATUS)
       and c.materialcode = i_materiel_code;

  end p_loc_stat_apple_main;

  procedure p_loc_stat_apple_saled(i_region                 IN number,
                                   i_orgid                  IN varchar2,
                                   i_res_typeid             IN varchar2,
                                   o_saleQuantity           OUT number,
                                   o_saleQuantity_added     OUT number,
                                   o_saleQuantity_decrement OUT number) is
  begin
    --已售库存总量
    select count(1)
      into o_saleQuantity
      from IM_INV_MOBTEL
     where region = i_region
       and ORG_ID = i_orgid
       and RES_TYPE_ID = i_res_typeid
       and INV_STATUS = 'OUTSTORE';

    --已售库存当日增加数量
    select count(1)
      into o_saleQuantity_added
      from IM_INV_MOBTEL
     where region = i_region
       and ORG_ID = i_orgid
       and RES_TYPE_ID = i_res_typeid
       and INV_STATUS = 'OUTSTORE'
       and status_date > sysdate - 1;
    --已售库存当日减少数量
    SELECT COUNT(1)
      into o_saleQuantity_decrement
      FROM IM_INV_MOBTEL a
     WHERE a.org_id = i_orgid
       AND a.res_type_id = i_res_typeid
       AND EXISTS (SELECT 1
              FROM im_busi_opera b, IM_SALE_DETAIL c
             WHERE a.region = b.region
               AND b.region = c.region
               AND c.inv_id = a.inv_id
               AND b.res_kind_id = 'rsclM'
               AND b.busi_type IN ('UNSALE', 'SALE_ROLLBACK')
               AND b.begin_date > SYSDATE - 1
               AND b.busi_id > to_char(SYSDATE - 1, 'YYMMDD')
               AND b.busi_id = c.busi_id);

  end p_loc_stat_apple_saled;
end PKG_LOC_STAT_APPLE;
/

delimiter ;/



delimiter /;
CREATE OR REPLACE PACKAGE "IM"."PKG_GET_ROUTE_INFO" as
type rc_route_info is ref cursor;
  procedure ap_get_route_info(
     v_local_sysid in  varchar2,	--发起调用的region的sysid
     v_areacode    in  varchar2,	--业务地的地区编号
     v_opcode      in  varchar2,	--业务操作码
     v_rc          out rc_route_info --返回的结果集
  );
end pkg_get_route_info;
/

CREATE OR REPLACE PACKAGE BODY "IM"."PKG_GET_ROUTE_INFO" as
procedure ap_get_route_info(
     v_local_sysid in  varchar2,	--发起调用的region的sysid
     v_areacode    in  varchar2,	--业务地的地区编号
     v_opcode      in  varchar2,	--业务操作码
     v_rc          out rc_route_info --返回的结果集
) as
    sqlstr   VARCHAR2 (1024);
    l_str varchar2(128);
    l_hostn1 varchar2(128);
    l_args varchar2(256);
    v_program     varchar2(128);	--程序名称
    v_transid     varchar2(128);	--交易类名称
begin
    l_args := 'local_sysid='||v_local_sysid||'; areacode='||v_areacode||'; opcode='||v_opcode;
    begin
        select hostname into l_hostn1
        from cics_region_route
        where sysid=v_local_sysid and rownum = 1;
      exception
        when no_data_found then
            raise_application_error(-20001, '在表cics_region_route中没有定义sysid ['||v_local_sysid||']对应的路由信息！' ||chr(10)||
                                            '输入参数：'||l_args);
    end;
   v_program := null;
   l_str := '@'||v_areacode; --保留OpCode，表示地区默认路由
   for c in (select opcode, program, transid from cics_opcode_route where opcode in(v_opcode, l_str))
   loop
       v_program := c.program;
       v_transid := c.transid;
       if c.opcode = v_opcode then
           goto OUT_POINT;
       end if;
   end loop;
   <<OUT_POINT>>
      null;
   if v_program is null then
       raise_application_error(-20001, '在表cics_opcode_route中没有定义OpCode['||v_opcode||']的路由信息！region:'||v_areacode||chr(10)||
                                       '输入参数：'||l_args);
   end if;
    begin
         sqlstr := 'select sysid, decode(hostname,:v_hostname,''true'',''false'') samehost,decode(hostname,'''||l_hostn1||''',0,SERIALNO) SERIALNO,PROGRAM,:v_transid TRANSID
                 from cics_region_route
                 where region=:v_areacode
                 and PROGRAM =:v_program
                 order by SERIALNO';
         OPEN v_rc FOR sqlstr USING l_hostn1,v_transid,v_areacode,v_program;
     exception
        when no_data_found then
            raise_application_error(-20001, '在表cics_region_route中没有定义地区['||v_areacode||']程序['||v_program||']的路由信息！' ||chr(10)||
                                            '输入参数：'||l_args);
   end;
  end ap_get_route_info;
end pkg_get_route_info;
/

delimiter ;/