CREATE OR REPLACE PROCEDURE "LOC_FUN_PHAPPLY" IS
BEGIN
  --删除排行表中当前数据
  DELETE FROM psi_info_mob_rankinglist t WHERE t.ranking_type = 'pickRankFan';
  ---统计最近30日提货数据
  FOR v_record IN (SELECT /*+parallel(6) enable_parallel_dml*/ *
                     FROM (

                           SELECT A.AMT, B.RES_TYPE_ID, B.BRAND_ID
                             FROM (SELECT SUM(B.APPLY_NUM) AMT,
                                           NVL(SUBSTR(RES_TYPE_ID, 0, INSTR(RES_TYPE_ID, '_') - 1),
                                               RES_TYPE_ID) RES_TYPE_ID
                                      FROM PSI_FLOW_APPLY_BATCH B
                                     WHERE B.CREATE_DATE > TO_DATE('20300101', 'yyyy-mm-dd')
                                    --AND SETTLE_MODE = 'SELFSALE'
                                     GROUP BY NVL(SUBSTR(RES_TYPE_ID, 0, INSTR(RES_TYPE_ID, '_') - 1),
                                                  RES_TYPE_ID)
                                     ORDER BY SUM(B.APPLY_NUM) DESC) A
                             LEFT JOIN PSI_INFO_RESTYPE_BUFFER B ON INSTR(B.RES_TYPE_ID, A.RES_TYPE_ID) > 0
                                                                AND INSTR(B.RES_TYPE_ID, '_PH') > 0
                            WHERE B. RES_TYPE_ID IS NOT NULL
                            GROUP BY A.AMT, B.RES_TYPE_ID, B.BRAND_ID

                            ORDER BY A.AMT DESC)
                    WHERE ROWNUM <= 30) LOOP
  dbms_output.put_line('-->v_record：AMT' || v_record.AMT);
  dbms_output.put_line('-->v_record：RES_TYPE_ID=' || v_record.RES_TYPE_ID);
  dbms_output.put_line('-->v_record：BRAND_ID=' || v_record.BRAND_ID);
  dbms_output.put_line('-->v_record：开始插入数据');
    INSERT INTO psi_info_mob_rankinglist
      (Ranking_type,
       Res_type_id,
       Brand_id,
       Static_amt,
       Static_amt1,
       Static_amt2,
       Static_amt3,
       Org_id)
    VALUES
      ('pickRankFan', v_record.res_type_id, v_record.brand_id, v_record.amt, NULL, NULL, NULL, NULL);
    COMMIT;
  dbms_output.put_line('-->v_record：插入数据成功');
  END LOOP;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
END;
/

CREATE OR REPLACE PROCEDURE "LOC_FUN_PHSALE" IS
BEGIN
  --删除排行表中当前数据
  DELETE FROM psi_info_mob_rankinglist t WHERE t.ranking_type = 'sellRankFan';
  ---统计最近30日提货数据
  FOR v_record IN (SELECT /*+parallel(6) enable_parallel_dml*/ *
                     FROM (
                           SELECT A.AMT, B.RES_TYPE_ID, B.BRAND_ID
                             FROM (SELECT NVL(SUBSTR(RES_TYPE_ID, 0, INSTR(RES_TYPE_ID, '_') - 1),
                                               RES_TYPE_ID) RES_TYPE_ID,
                                           SUM(b.sale_num) amt
                                      FROM psi_stat_daypsi b
                                     WHERE b.create_date > TO_DATE('20300101', 'yyyy-mm-dd')
                                     GROUP BY NVL(SUBSTR(RES_TYPE_ID, 0, INSTR(RES_TYPE_ID, '_') - 1),
                                                  RES_TYPE_ID)) A
                             LEFT JOIN PSI_INFO_RESTYPE_BUFFER B ON INSTR(B.RES_TYPE_ID, A.RES_TYPE_ID) > 0
                                                                AND INSTR(B.RES_TYPE_ID, '_PH') > 0
                            WHERE B. RES_TYPE_ID IS NOT NULL
                            GROUP BY A.AMT, B.RES_TYPE_ID, B.BRAND_ID
                            ORDER BY A.AMT DESC)
                    WHERE ROWNUM <= 30) LOOP
          dbms_output.put_line('-->v_record：AMT=' || v_record.AMT);
          dbms_output.put_line('-->v_record：RES_TYPE_ID=' || v_record.RES_TYPE_ID);
          dbms_output.put_line('-->v_record：BRAND_ID=' || v_record.BRAND_ID);
          dbms_output.put_line('-->v_record：开始插入数据');
    INSERT INTO psi_info_mob_rankinglist
      (Ranking_type,
       Res_type_id,
       Brand_id,
       Static_amt,
       Static_amt1,
       Static_amt2,
       Static_amt3,
       Org_id)
    VALUES
      ('sellRankFan', v_record.res_type_id, v_record.brand_id, v_record.amt, NULL, NULL, NULL, NULL);
    COMMIT;
  dbms_output.put_line('-->v_record：插入数据成功');
  END LOOP;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
END;
/


CREATE OR REPLACE PROCEDURE "LOC_PRC_CLEARFLOWAPPLY" IS
  V_SYSDATE DATE := SYSDATE;
BEGIN
  --订单表超过一年归入历史表
  INSERT /*+parallel(6) enable_parallel_dml*/ INTO PSI_FLOW_APPLY_BATCH_HIS
    SELECT REGION,BATCH_ID,FLOW_ID,RES_TYPE_ID,COLOR,SUPPLIER_ID,APPLY_NUM,ARRIVE_NUM,RECEIVE_ORG,ORDER_ID,STATUS,MEMO,BRAND_ID,SETTLE_MODE,RECEIVE_ADDR,ORDER_DETAIL_ID,ORIG_NUM,CREATE_DATE,RECEIVE_DATE,RECEIVE_LAST_DATE,STOCKUP_NUM,RWD_LEVEL_ID,TOTAL_PRICE,TOTAL_DISC,UNIT_PRICE,IS_GIFT,GIFT_BATCH_ID,MAX_APPLY_NUMBER,PRIV_DESC,MAPPING_RESTYPE
      FROM PSI_FLOW_APPLY_BATCH T
     WHERE T.CREATE_DATE < ADD_MONTHS(TO_DATE('20100101','yyyy-mm-dd'), -12);
  dbms_output.put_line('-->1订单表超过一年归入历史：向表中 PSI_FLOW_APPLY_BATCH_HIS 插入数据');
  DELETE PSI_FLOW_APPLY_BATCH T
   WHERE T.CREATE_DATE < ADD_MONTHS(V_SYSDATE, -12);

  --批次表超过一年归入历史表
  INSERT /*+parallel(6) enable_parallel_dml*/ INTO PSI_FLOW_APPLY_HIS
    SELECT REGION,FLOW_ID,FLOW_STATUS,RES_KIND_ID,ORG_LEVEL,ORG_ID,APPLY_CYCLE,CYCLE_TYPE,APPLY_TYPE,APPLY_CLASS,CHANNEL_TYPE,PROJECT_ID,IF_BASE,PARENT_FLOW_ID,LINK_MAN,CONTACT_PHONE,RECEIVE_DATE,RECEIVE_LAST_DATE,CREATE_DATE,CREATE_OPER,CREATE_ORG_ID,DES_ORG_ID,MEMO,LOCK_STATUS,STATUS_DATE,DES_REGION,SRC_ORG_ID,DES_STORE_ID,SRC_STORE_ID,ADDRESS,SUM_PRICE,PAY_TYPE,PAY_OID,SUM_APPLY_NUM,PAY_PRICE,ORDER_PRIV_ID,ORDER_PRIV_DESC,ORDER_PRIV_DISC,PAY_STATUS,UNLOCKED_EXPDATE,OTHER_COUPONS,ALREADY_PAY_PRICE,PAYPLAT_TYPE,COUNTY_ID,SUPPLIER_ID,SETTLE_MODE,CLOSE_FLAG,ADD_STATUS,COMMENT_TIMES,OUT_ORDER_ID
      FROM PSI_FLOW_APPLY T
     WHERE T.CREATE_DATE < ADD_MONTHS(TO_DATE('20100101','yyyy-mm-dd'), -12);
  dbms_output.put_line('-->2批次表超过一年归入历史表：向表中 PSI_FLOW_APPLY_HIS 插入数据'); 
  DELETE PSI_FLOW_APPLY T WHERE T.CREATE_DATE < ADD_MONTHS(V_SYSDATE, -12);

  --每月初清理前一个月的撤销订单
  INSERT /*+parallel(6) enable_parallel_dml*/ INTO PSI_FLOW_APPLY_BATCH_HIS
    SELECT REGION,BATCH_ID,FLOW_ID,RES_TYPE_ID,COLOR,SUPPLIER_ID,APPLY_NUM,ARRIVE_NUM,RECEIVE_ORG,ORDER_ID,STATUS,MEMO,BRAND_ID,SETTLE_MODE,RECEIVE_ADDR,ORDER_DETAIL_ID,ORIG_NUM,CREATE_DATE,RECEIVE_DATE,RECEIVE_LAST_DATE,STOCKUP_NUM,RWD_LEVEL_ID,TOTAL_PRICE,TOTAL_DISC,UNIT_PRICE,IS_GIFT,GIFT_BATCH_ID,MAX_APPLY_NUMBER,PRIV_DESC,MAPPING_RESTYPE
      FROM PSI_FLOW_APPLY_BATCH T
     WHERE TRUNC(T.CREATE_DATE, 'MM') <
           TRUNC(ADD_MONTHS(TO_DATE('20100101','yyyy-mm-dd'), -1), 'MM')
       AND EXISTS
     (SELECT 1
              FROM PSI_FLOW_APPLY A
             WHERE A.FLOW_ID = T.FLOW_ID
               AND A.FLOW_STATUS = 'cancel'
               AND TRUNC(A.CREATE_DATE, 'MM') <
                   TRUNC(ADD_MONTHS(TO_DATE('20100101','yyyy-mm-dd'), -1), 'MM'));
  dbms_output.put_line('-->3每月初清理前一个月的撤销订单：向表中 PSI_FLOW_APPLY_BATCH_HIS 插入数据');
  DELETE PSI_FLOW_APPLY_BATCH T
   WHERE TRUNC(T.CREATE_DATE, 'MM') <
         TRUNC(ADD_MONTHS(V_SYSDATE, -1), 'MM')
     AND EXISTS (SELECT 1
            FROM PSI_FLOW_APPLY A
           WHERE A.FLOW_ID = T.FLOW_ID
             AND A.FLOW_STATUS = 'cancel'
             AND TRUNC(A.CREATE_DATE, 'MM') <
                 TRUNC(ADD_MONTHS(V_SYSDATE, -1), 'MM'));
  INSERT /*+parallel(6) enable_parallel_dml*/ INTO PSI_FLOW_APPLY_HIS
    SELECT REGION,FLOW_ID,FLOW_STATUS,RES_KIND_ID,ORG_LEVEL,ORG_ID,APPLY_CYCLE,CYCLE_TYPE,APPLY_TYPE,APPLY_CLASS,CHANNEL_TYPE,PROJECT_ID,IF_BASE,PARENT_FLOW_ID,LINK_MAN,CONTACT_PHONE,RECEIVE_DATE,RECEIVE_LAST_DATE,CREATE_DATE,CREATE_OPER,CREATE_ORG_ID,DES_ORG_ID,MEMO,LOCK_STATUS,STATUS_DATE,DES_REGION,SRC_ORG_ID,DES_STORE_ID,SRC_STORE_ID,ADDRESS,SUM_PRICE,PAY_TYPE,PAY_OID,SUM_APPLY_NUM,PAY_PRICE,ORDER_PRIV_ID,ORDER_PRIV_DESC,ORDER_PRIV_DISC,PAY_STATUS,UNLOCKED_EXPDATE,OTHER_COUPONS,ALREADY_PAY_PRICE,PAYPLAT_TYPE,COUNTY_ID,SUPPLIER_ID,SETTLE_MODE,CLOSE_FLAG,ADD_STATUS,COMMENT_TIMES,OUT_ORDER_ID
      FROM PSI_FLOW_APPLY T
     WHERE T.FLOW_STATUS = 'cancel'
       AND TRUNC(T.CREATE_DATE, 'MM') <
           TRUNC(ADD_MONTHS(TO_DATE('20100101','yyyy-mm-dd'), -1), 'MM');
  dbms_output.put_line('-->4每月初清理前一个月的撤销订单：向表中 PSI_FLOW_APPLY_HIS 插入数据');
  DELETE PSI_FLOW_APPLY T
   WHERE T.FLOW_STATUS = 'cancel'
     AND TRUNC(T.CREATE_DATE, 'MM') <
         TRUNC(ADD_MONTHS(V_SYSDATE, -1), 'MM');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(SQLERRM);
END LOC_PRC_CLEARFLOWAPPLY;
/

CREATE OR REPLACE PROCEDURE "P_LOC_IMEISCAN"
  IS
BEGIN
  -- 把 PSI_LOG_IMEISCAN 表中失效的数据添加到历史表 PSI_LOG_IMEISCAN_HIS
  INSERT /*+parallel(6) enable_parallel_dml*/ INTO PSI_LOG_IMEISCAN_HIS(REGION,       -- 地市
                                   SCAN_BATCHID, -- 批次流水,每次扫描一次流水
                                   SCAN_TYPE,    -- 扫描类型  自备机入库 ZBJSCAN
                                   ORG_ID,       -- 扫描单位
                                   OPER_ID,      -- 扫描操作员
                                   INV_ID,       -- 串号
                                   CREATE_DATE,  -- 扫描日期
                                   EXP_DATE,     -- 失效时间
                                   RELA_FLOW_ID, -- 关联流水
                                   STATUS,       -- 状态
                                   STATUS_DATE)  -- 状态时间
  SELECT T.REGION,
         T.SCAN_BATCHID,
         T.SCAN_TYPE,
         T.ORG_ID,
         T.OPER_ID,
         T.INV_ID,
         T.CREATE_DATE,
         T.EXP_DATE,
         T.RELA_FLOW_ID,
         'OVERTIME',
         sysdate
    FROM PSI_LOG_IMEISCAN T
   WHERE T.EXP_DATE <= sysdate;
    
  -- 把 PSI_LOG_IMEISCAN 表中失效的数据添删除
  DELETE FROM PSI_LOG_IMEISCAN WHERE EXP_DATE <= sysdate;

  COMMIT;
END P_LOC_IMEISCAN;
/

CREATE OR REPLACE PROCEDURE "PRC_LOC_EXCEEDINFO" (v_region in number) is
  v_topvalue number(10);
begin
  --删除历史记录
  delete from loc_type_exceedinfo;
  delete from psi_info_mob_rankinglist t
   where t.ranking_type = 'EXCEEDINFO' and t.region=v_region;
  --查询库存量
  for v_record in (select /*+parallel(6) enable_parallel_dml*/ t.region,
                          t.org_id,
                          t.res_type_id,
                          count(t.inv_id) cnt
                     from im_inv_mobtel t
                    where t.region = v_region
                      and t.inv_status = 'INSTORE'
                      and t.settle_mode = 'SELFSALE'
                    group by t.region, t.org_id, t.res_type_id) loop
    --查询高水值
    begin
   dbms_output.put_line('-->v_record：region='||v_record.region || '  org_id='||v_record.org_id || '  res_type_id='||v_record.res_type_id || '  cnt='||v_record.cnt);
      select t.static_amt2
        into v_topvalue
        from psi_info_mob_rankinglist t
       where t.ranking_type = 'SALEINFO'
         and t.region = v_region
         and t.org_id = v_record.org_id
         and t.ranking_type = v_record.res_type_id;
   dbms_output.put_line('-->v_record：v_topvalue='||v_topvalue);
    exception
      when others then
        v_topvalue := 0;
    end;
  dbms_output.put_line('-->查询高水值：v_topvalue='||v_topvalue);
    --向中间表插入超限厅
    if v_record.cnt > v_topvalue then
    dbms_output.put_line('-->向中间表插入超限厅：cnt='||v_record.cnt || ' v_topvalue='||v_topvalue);
      insert into loc_type_exceedinfo
        (REGION, AREAID, ORG_ID, RES_TYPE_ID)
      values
        (v_region,
         substr(v_record.org_id, 0, 8),
         v_record.org_id,
         v_record.res_type_id);
      --向排行表插入超限厅
      insert into psi_info_mob_rankinglist
        (RANKING_TYPE,
         RES_TYPE_ID,
         BRAND_ID,
         STATIC_AMT,
         STATIC_AMT1,
         STATIC_AMT2,
         STATIC_AMT3,
         ORG_ID,
         ATTR1,
         ATTR2,
         STATIC_CAMT,
         STATIC_CAMT1,
         STATIC_CAMT2,
         STATIC_CAMT3,
         OID,
         REGION)
      values
        ('EXCEEDINFO',
         v_record.res_type_id,
         null,
         1,
         null,
         null,
         null,
         v_record.org_id,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         v_record.region);
    dbms_output.put_line('-->向中间表插入超限厅：表 loc_type_exceedinfo 和 psi_info_mob_rankinglist 数据插入成功');
    end if;
  end loop;
  commit;
  --统计超限县
  for v_record1 in (select /*+parallel(6) enable_parallel_dml*/ t.region,
                           t.areaid,
                           t.res_type_id,
                           sum(t.res_type_id) cnt
                      from loc_type_exceedinfo t
                     group by t.region, t.areaid, t.res_type_id) loop
    dbms_output.put_line('-->v_record1：region='||v_record1.region || ' areaid='||v_record1.areaid || ' res_type_id='||v_record1.res_type_id || ' cnt='||v_record1.cnt);
    --统计县的高水值
    select sum(t.static_amt2)
      into v_topvalue
      from psi_info_mob_rankinglist t
     where t.ranking_type = 'SALEINFO'
       and t.region = v_region
       and substr(t.org_id, 0, 8) = v_record1.areaid
       and t.ranking_type = v_record1.res_type_id;
   dbms_output.put_line('-->v_record1：v_topvalue='||v_topvalue);
    --插入超限区县到排行表
    if v_record1.cnt > v_topvalue then
    dbms_output.put_line('-->插入超限区县到排行表：cnt='||v_record1.cnt || ' v_topvalue='||v_topvalue);
      insert into psi_info_mob_rankinglist
        (RANKING_TYPE,
         RES_TYPE_ID,
         BRAND_ID,
         STATIC_AMT,
         STATIC_AMT1,
         STATIC_AMT2,
         STATIC_AMT3,
         ORG_ID,
         ATTR1,
         ATTR2,
         STATIC_CAMT,
         STATIC_CAMT1,
         STATIC_CAMT2,
         STATIC_CAMT3,
         OID,
         REGION)
      values
        ('EXCEEDINFO',
         v_record1.res_type_id,
         null,
         null,
         1,
         null,
         null,
         v_record1.areaid,
         null,
         null,
         null,
         null,
         null,
         null,
         null,
         v_record1.region);
     dbms_output.put_line('-->插入超限区县到排行表：表 psi_info_mob_rankinglist 数据插入成功');
    end if;
  end loop;
  commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end prc_loc_exceedinfo;
/

CREATE OR REPLACE PROCEDURE "PRC_LOC_SALEVOL_RANK" (v_region in number) is
begin
  delete from psi_info_mob_rankinglist t where t.ranking_type = 'SALEINFO'and region=v_region;
  ----最近30天销量
  for v_record in (select /*+parallel(6) enable_parallel_dml*/ t.region,
                          t.org_id,
                          t.res_type_id,
                          sum(t.sale_num) amt
                     from psi_stat_daypsi t
                    where t.region = v_region
                      and t.create_date > sysdate - 30
                      and t.Org_Type = 'SELF'
                    group by region, org_id, res_type_id) loop
  dbms_output.put_line('-->v_record1：region=' || v_record.region ||' org_id=' || v_record.org_id || ' res_type_id=' || v_record.res_type_id || ' amt='||v_record.amt);
    ---插入30天销量、枯水值、参考订单量
    insert into psi_info_mob_rankinglist
      (RANKING_TYPE,
       RES_TYPE_ID,
       BRAND_ID,
       STATIC_AMT,
       STATIC_AMT1,
       STATIC_AMT2,
       STATIC_AMT3,
       ORG_ID,
       ATTR1,
       ATTR2,
       STATIC_CAMT,
       STATIC_CAMT1,
       STATIC_CAMT2,
       STATIC_CAMT3,
       OID,
       REGION)
    values
      ('SALEINFO',
       v_record.res_type_id,
       '',
       v_record.amt,
       v_record.amt / 30 * 3,
       '',
       v_record.amt / 30 * 15,
       v_record.org_id,
       '',
       '',
       '',
       '',
       '',
       '',
       '',
       v_record.region);
   dbms_output.put_line('-->v_record1：表 psi_info_mob_rankinglist 数据插入成功');
  end loop;
  commit;
  ----最近60天销量
  for v_record in (select /*+parallel(6) enable_parallel_dml*/ t.region,
                          t.org_id,
                          t.res_type_id,
                          sum(t.sale_num) amt
                     from psi_stat_daypsi t
                    where t.region = v_region
                      and t.create_date > sysdate - 60
                      and t.Org_Type = 'SELF'
                    group by region, org_id, res_type_id) loop
  dbms_output.put_line('-->v_record2：region=' || v_record.region ||' org_id=' || v_record.org_id || ' res_type_id=' || v_record.res_type_id || ' amt='||v_record.amt);
   
    --插入高水值
    update psi_info_mob_rankinglist t
       set t.static_amt2 = v_record.amt / 60 * 30
     where t.region = v_record.region
       and t.res_type_id = v_record.res_type_id
       and t.org_id = v_record.org_id;
   dbms_output.put_line('-->v_record2：表 psi_info_mob_rankinglist 数据更新成功');
    --值都为0时不记录此指标
    delete from psi_info_mob_rankinglist t
     where t.static_amt = 0
       and t.static_amt2 = 0;
  end loop;
  commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end prc_loc_salevol_rank;
/

CREATE OR REPLACE PROCEDURE "PRC_PSI_CTRM_LOGINTOKEN_CLEAR" (v_region number)
is
 begin
   insert /*+parallel(6) enable_parallel_dml*/ into Psi_ctrm_logintoken_his
     select * from Psi_ctrm_logintoken t
       where t.region = v_region
         and t.Create_date <= sysdate-1/288;

     delete from Psi_ctrm_logintoken t
       where t.region = v_region
         and t.Create_date <= sysdate-1/288;
     commit;
end PRC_PSI_CTRM_LOGINTOKEN_CLEAR;
/


CREATE OR REPLACE PROCEDURE "VIP_CRAD_INIT" (v_region  IN VARCHAR2,
                                            v_rtype   IN VARCHAR2,
                                            v_store   IN VARCHAR2,
                                            v_ctype   IN NUMBER,
                                            v_ctypeid IN VARCHAR2,
                                            v_count   IN NUMBER) AS
  v_tmp  VARCHAR2(100);
  v_flag NUMBER;
BEGIN
  FOR i IN 1 .. v_count LOOP
    IF (Instr(i, '4', 1, 1) = 0 AND Instr(i, '7', 1, 1) = 0)
    THEN
      SELECT 15 || to_char(to_date('20201014','yyyy-mm-dd'), 'YY') || to_char(to_date('20201014','yyyy-mm-dd'), 'MM') || v_ctype ||
             v_rtype || LPad(i, 7, 0)
        INTO v_tmp
        FROM dual;
      SELECT COUNT(*) INTO v_flag FROM im_inv_realsign WHERE inv_id = v_tmp;
    dbms_output.put_line('--> v_tmp='||v_tmp || ' v_flag='||v_flag);
      IF v_flag = 0
      THEN
        INSERT INTO im_inv_realsign
          (REGION,
           INV_ID,
           RES_TYPE_ID,
           INV_STATUS,
           CHECK_STATUS,
           STATUS_DATE,
           STATUS_REASON,
           LDSTORE_BUSI_ID,
           LAST_BUSI_ID,
           STORE_ID,
           ORG_ID,
           PHYSICAL_STATUS,
           BUSI_STATUS,
           OPER_ID,
           USE_ORG_ID,
           ASSET_ID,
           SUPPLIER_ID,
           FORMAT_ID,
           MEMO,
           ADDDATA1,
           ADDDATA2,
           ADDDATA3,
           ADDDATA4,
           ADDDATA5,
           LDSTORE_DATE,
           COST_PRICE)
        VALUES
          (v_region,
           v_tmp,
           v_ctypeid,
           'INSTORE',
           'NORMAL',
           SYSDATE,
           'PURCHASE',
           to_char(SYSDATE, 'YYYYMMDDHHMMSS'),
           to_char(SYSDATE, 'YYYYMMDDHHMMSS'),
           'SD.L' || v_store,
           'SD.L' || v_store,
           'NORMAL',
           'USABLE',
           '00000000',
           'SD.L' || v_store,
           '',
           'UNKNOWN',
           '',
           '',
           '',
           '',
           '',
           '',
           '',
           SYSDATE,
           '0');
     dbms_output.put_line('-->当v_flag=0时，表 im_inv_realsign 中的数据插入成功');
      ELSE
        dbms_output.put_line('库中已存在:' || v_tmp);
      END IF;
      IF MOD(i, 100) = 0
      THEN
        COMMIT;
      END IF;
    END IF;
  END LOOP;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('->异常原因:' || v_tmp);
END;
/

CREATE OR REPLACE PROCEDURE "AP_IM_INITICCID" (IFORCE NUMBER DEFAULT '0')
as
  iDate varchar(4);
  iYear varchar(4);
begin
  select to_char (sysdate,'MMDD') into iDate from dual;
  select to_char (sysdate,'YYYY') into iYear from dual;
  if iDate != '0101'  then
    if  iForce = 0 then
      dbms_output.put_line('-->iDate='||iDate || ' iForce='||iForce);
      return;
    end if;
  end if;

  insert /*+parallel(6) enable_parallel_dml*/ into im_iccid_rule(rule_id,vendor_id,msisdn_number,year,mcc,ss,m,f,current_iccid,create_date,oper_id)
    select to_char(sysdate, 'yymmddhhmi') ||lpad(to_char(SEQ_BUSI_ID.nextval), 14, '0') rule_id,
          a.vendor_id,
          a.msisdn_number,
          to_char(sysdate, 'yyyy') year,
          a.mcc,
          a.ss,
          a.m,
          a.f,
          '0000000' current_iccid,
          sysdate create_date,
          'sys' oper_id
     from (select t.vendor_id, t.msisdn_number, t.mcc, t.ss, t.m, t.f
             from im_iccid_rule t
            where t.year = to_char(add_months(sysdate, -12), 'yyyy')
           minus
           select t.vendor_id, t.msisdn_number, t.mcc, t.ss, t.m, t.f
             from im_iccid_rule t
            where t.year = to_char(sysdate, 'yyyy')) a;
  dbms_output.put_line('-->表 im_iccid_rule 数据插入成功');
       insert into im_cfg_blankcard_serial
         (vendor_id,
          y1y2,
          p1p2,
          l1l2,
          m1m2,
          cur_serial,
          create_date,
          create_oper)
         select a.vendor_id,
                to_char(sysdate, 'yy') Y1Y2,
                a.P1P2,
                a.L1L2,
                a.M1M2,
                '0000000' cur_serial,
                sysdate,
                'sys' create_oper
           from (select t.vendor_id, t.p1p2, t.l1l2, t.m1m2
                   from im_cfg_blankcard_serial t
                  where t.y1y2 = to_char(add_months(sysdate, -12), 'yy')
                 minus
                 select t.vendor_id, t.p1p2, t.l1l2, t.m1m2
                   from im_cfg_blankcard_serial t
                  where t.y1y2 = to_char(sysdate, 'yy')) a;
  dbms_output.put_line('-->表 im_cfg_blankcard_serial 数据插入成功');
  commit;
  dbms_output.put_line('-->程序执行结束');
end;
/

CREATE OR REPLACE PROCEDURE "AP_IM_PBOSS_ROUTERTONODE" IS
BEGIN
FOR LOOP_REGION IN (SELECT REGION FROM REGION_LIST) LOOP
  dbms_output.put_line('-->for循环中：REGION='||LOOP_REGION.REGION);
--插入近端节点
  INSERT INTO IM_PRES_SDH_TOPOLOGYNODE
    (REGION,NODE_ID,NODE_NAME,NODE_TYPE,DEVICE_TYPE,DEVICE_ID,POS_X,POS_Y,MAPID,STATUS,CREATE_DATE,OPER_ID,SHAPE_TYPE,STATUS_DATE,ORG_NODE_ID)
    SELECT T.REGION,SEQ_IM_PRES_OID.NEXTVAL,T.LOCAL_NODE_DESC,'TRANSNODE',NULL,NULL,0,0,NULL,'USED',SYSDATE,'BOSSROLE',NULL,SYSDATE,T.ORG_LOCAL_NODE
      FROM (SELECT REGION, LOCAL_NODE_DESC, ORG_LOCAL_NODE
              FROM IM_PRES_SDH_TOPOLOGYROUTER
             WHERE LOCAL_NODE_DESC IS NOT NULL
               AND ORG_LOCAL_NODE IS NOT NULL
               AND REGION =LOOP_REGION.REGION
             GROUP BY REGION, LOCAL_NODE_DESC, ORG_LOCAL_NODE) T
     WHERE NOT EXISTS (SELECT 1
              FROM IM_PRES_SDH_TOPOLOGYNODE A
             WHERE A.ORG_NODE_ID = T.ORG_LOCAL_NODE AND REGION = LOOP_REGION.REGION);
  dbms_output.put_line('-->1 插入近端节点：表 IM_PRES_SDH_TOPOLOGYNODE 数据插入成功');
--插入远端节点
  INSERT INTO IM_PRES_SDH_TOPOLOGYNODE
    (REGION,NODE_ID,NODE_NAME,NODE_TYPE,DEVICE_TYPE,DEVICE_ID,POS_X,POS_Y,MAPID,STATUS,CREATE_DATE,OPER_ID,SHAPE_TYPE,STATUS_DATE,ORG_NODE_ID)
    SELECT T.REGION,SEQ_IM_PRES_OID.NEXTVAL,T.REMOTE_NODE_DESC,'TRANSNODE',NULL,NULL,0,0,NULL,'USED',SYSDATE,'BOSSROLE',NULL,SYSDATE,T.ORG_REMOTE_NODE
      FROM (SELECT REGION, REMOTE_NODE_DESC, ORG_REMOTE_NODE
              FROM IM_PRES_SDH_TOPOLOGYROUTER
             WHERE REMOTE_NODE_DESC IS NOT NULL
               AND ORG_REMOTE_NODE IS NOT NULL
               AND REGION = LOOP_REGION.REGION
             GROUP BY REGION, REMOTE_NODE_DESC, ORG_REMOTE_NODE) T
     WHERE NOT EXISTS (SELECT 1
              FROM IM_PRES_SDH_TOPOLOGYNODE A
             WHERE A.ORG_NODE_ID = T.ORG_REMOTE_NODE AND REGION =LOOP_REGION.REGION);
  dbms_output.put_line('-->2 插入远端节点：表 IM_PRES_SDH_TOPOLOGYNODE 数据插入成功');
--更新节点表设备ID,更新节点表设备类型
UPDATE IM_PRES_SDH_TOPOLOGYNODE T
   SET (T.DEVICE_ID,T.DEVICE_TYPE) = (SELECT A.SDH_ID,A.DEVICE_TYPE
                        FROM IM_PRES_SDHINFO A
                       WHERE A.ORG_SDH_ID = T.ORG_NODE_ID
                         AND A.REGION = LOOP_REGION.REGION)
 WHERE EXISTS (SELECT 1
          FROM IM_PRES_SDHINFO S
         WHERE S.ORG_SDH_ID = T.ORG_NODE_ID
           AND S.REGION = LOOP_REGION.REGION)
   AND T.REGION = LOOP_REGION.REGION;
  
  dbms_output.put_line('-->3 更新节点表设备类型：表 IM_PRES_SDH_TOPOLOGYNODE 数据更新成功');
--更新路由表中 近端node id
UPDATE IM_PRES_SDH_TOPOLOGYROUTER T
   SET T.LOCAL_NODE = (SELECT A.NODE_ID
                         FROM IM_PRES_SDH_TOPOLOGYNODE A
                        WHERE A.ORG_NODE_ID = T.ORG_LOCAL_NODE
                          AND A.REGION = LOOP_REGION.REGION)
 WHERE EXISTS (SELECT 1
         FROM IM_PRES_SDH_TOPOLOGYNODE A
         WHERE A.ORG_NODE_ID = T.ORG_LOCAL_NODE
             AND A.REGION = LOOP_REGION.REGION)
   AND T.REGION = LOOP_REGION.REGION;
  dbms_output.put_line('-->4 更新路由表中近端node id：表 IM_PRES_SDH_TOPOLOGYROUTER 数据更新成功');
--更新路由表中 远端node id
UPDATE IM_PRES_SDH_TOPOLOGYROUTER T
   SET T.REMOTE_NODE = (SELECT A.NODE_ID
                          FROM IM_PRES_SDH_TOPOLOGYNODE A
                         WHERE A.ORG_NODE_ID = T.ORG_REMOTE_NODE
                           AND A.REGION = LOOP_REGION.REGION)
 WHERE EXISTS (SELECT 1
       FROM IM_PRES_SDH_TOPOLOGYNODE A
       WHERE A.ORG_NODE_ID = T.ORG_REMOTE_NODE
             AND A.REGION = LOOP_REGION.REGION)
 AND T.REGION = LOOP_REGION.REGION;
  dbms_output.put_line('-->4 更新路由表中远端node：表 IM_PRES_SDH_TOPOLOGYROUTER 数据更新成功');
END LOOP;
COMMIT;
END AP_IM_PBOSS_ROUTERTONODE;
/

CREATE OR REPLACE PROCEDURE "AP_IM_PRES_FREELOCKDATA" AS
begin
    for loop_region in (select region from region_list) loop
    dbms_output.put_line('-->loop_region：region='||loop_region.region);
      begin
         update IM_PRES_SDHPORTINFO set status='IDLE' where status = 'LOCK' and region = loop_region.region and status_date < to_date('20201015','yyyy-mm-dd');
         update IM_PRES_SDHVC12 set status= 'IDLE', port_id = null where status= 'LOCK' and region = loop_region.region and status_date < to_date('20201015','yyyy-mm-dd');
      end;
    end loop;
  commit;
end AP_IM_PRES_FREELOCKDATA;
/

CREATE OR REPLACE PROCEDURE "LOC_IM_INV_REINFORCE" is
v_count NUMBER(10);
begin
--1区的处理
--循环处理各个地市和local_im_inv_reinforce_config配置表中的各种配置项
for v_region in (select region, orgid from region_list where region <> '999') loop
  dbms_output.put_line('-->v_region：region='||v_region.region|| ' orgid='||v_region.orgid);
    for c1 in (select limit_count,res_type_id from local_im_inv_reinforce_config where region = v_region.region) loop
    dbms_output.put_line('-->c1：limit_count='||c1.limit_count||' res_type_id'||c1.res_type_id);
select c1.limit_count - count(1) into v_count
  from im_inv_reinforce
 where region = to_number(v_region.region)
   and busi_status = 'USABLE'
   and inv_status = 'INSTORE'
   and store_id like 'SD.%'
   and res_type_id = c1.res_type_id;
  dbms_output.put_line('-->v_count='||v_count);
if v_count > 0 then
for rec in (select INV_ID from im_inv_reinforce t
 where t.region = 999
   and t.busi_status ='USABLE'
   and t.inv_status  ='INSTORE'
   and t.store_id like 'SD.%'
   and t.res_type_id = c1.res_type_id
   and rownum <= v_count) loop
   dbms_output.put_line('-->rec：INV_ID='||rec.INV_ID);
insert into im_inv_reinforce (REGION,INV_ID ,RES_TYPE_ID,INV_STATUS,CHECK_STATUS,STATUS_DATE,STATUS_REASON,LDSTORE_BUSI_ID,
  LAST_BUSI_ID, STORE_ID, ORG_ID,PHYSICAL_STATUS,BUSI_STATUS,PASSWORD,PASS_KEY,ACCT_ID,USABLE_MONEY,LAST_DATE,EFF_DATE,
  EXP_DATE,EFF_LIMIT,VC_ID,VC_STATUS,OPER_ID,USE_ORG_ID,CREATE_DATE,SUPPLIER_ID,FORMAT_ID,REGION_DELAY,PROVINCE_DELAY,
  MEMO,ADDDATA1,ADDDATA2,ADDDATA3,ADDDATA4,ADDDATA5,LDSTORE_DATE)
  (select v_region.region,INV_ID ,RES_TYPE_ID,INV_STATUS,CHECK_STATUS,STATUS_DATE,STATUS_REASON,LDSTORE_BUSI_ID,
  LAST_BUSI_ID, v_region.orgid, v_region.orgid,PHYSICAL_STATUS,BUSI_STATUS,PASSWORD,PASS_KEY,ACCT_ID,USABLE_MONEY,LAST_DATE,EFF_DATE,
  EXP_DATE,EFF_LIMIT,VC_ID,VC_STATUS,OPER_ID,USE_ORG_ID,CREATE_DATE,SUPPLIER_ID,FORMAT_ID,REGION_DELAY,PROVINCE_DELAY,
  MEMO,ADDDATA1,ADDDATA2,ADDDATA3,ADDDATA4,ADDDATA5,LDSTORE_DATE from im_inv_reinforce t
 where t.region = 999
   and t.INV_ID = rec.inv_id);

 delete from  im_inv_reinforce t
               where t.region = 999
                 and t.INV_ID = rec.inv_id;
  commit;
  dbms_output.put_line('-->表 im_inv_reinforce 中相应的数据删除成功');
  end loop;
end if;
  end loop;
end loop;
end loc_im_inv_reinforce;
/

CREATE OR REPLACE PROCEDURE "LOC_PRES_STDADDRESS" (i_region in number) is
 v_cnt NUMBER(2);
 begin
for cur_r in (select houseId, houseNm, cityNm , cityId , countyNm , countyId, townNm, townId, roadNm, roadId, zoneNm, zoneId, buildingNm, buildingId, unitNm, unitId, floorNm, floorId, fullAddr, substr(coverType,'1','1') coverType,PYCAPLOWER,FULLPYLOWER,PORTS,addr_type,cover_scenario from loc_im_mid_PRES_ADDR

  where cityId=i_region)
loop
    begin
  dbms_output.put_line('-->cur_r：cityId='||cur_r.cityId || ' houseId='||cur_r.houseId);
    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,addr_name,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.cityId,cur_r.cityId,1,cur_r.cityNm,-1,cur_r.cityId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.cityId and region=cur_r.cityId) and cur_r.cityId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.countyId,cur_r.countyId,2,cur_r.countyNm,cur_r.cityId,cur_r.cityId||'-'||cur_r.countyId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.countyId and region=cur_r.cityId) and cur_r.countyId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.townId,cur_r.townId,3,cur_r.townNm,cur_r.countyId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.townId and region=cur_r.cityId) and cur_r.townId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.roadId,cur_r.roadId,4,cur_r.roadNm,cur_r.townId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm||cur_r.roadNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.roadId and region=cur_r.cityId) and cur_r.roadId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME,PYCAPLOWER,FULLPYLOWER)
    select cur_r.cityId,cur_r.zoneId,cur_r.zoneId,5,cur_r.zoneNm,cur_r.roadId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm||cur_r.roadNm||cur_r.zoneNm,LOWER(cur_r.PYCAPLOWER),LOWER(cur_r.FULLPYLOWER)
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.zoneId and region=cur_r.cityId) and cur_r.zoneId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.buildingId,cur_r.buildingId,6,cur_r.buildingNm,cur_r.zoneId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId||'-'||cur_r.buildingId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm||cur_r.roadNm||cur_r.zoneNm||cur_r.buildingNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.buildingId and region=cur_r.cityId) and cur_r.buildingId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.unitId,cur_r.unitId,7,cur_r.unitNm,cur_r.buildingId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId||'-'||cur_r.buildingId||'-'||cur_r.unitId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm||cur_r.roadNm||cur_r.zoneNm||cur_r.buildingNm||cur_r.unitNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.unitId and region=cur_r.cityId) and cur_r.unitId is not null;

    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,ADDR_OTHER_NAME)
    select cur_r.cityId,cur_r.floorId,cur_r.floorId,8,cur_r.floorNm,cur_r.unitId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId||'-'||cur_r.buildingId||'-'||cur_r.unitId||'-'||cur_r.floorId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.cityNm||cur_r.countyNm||cur_r.townNm||cur_r.roadNm||cur_r.zoneNm||cur_r.buildingNm||cur_r.unitNm||cur_r.floorNm
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.floorId and region=cur_r.cityId) and cur_r.floorId is not null;

    select count(*) into v_cnt from im_pres_stdaddress  where region=i_region  and addr_id = cur_r.houseId ;
  dbms_output.put_line('-->v_cnt='||v_cnt);
    if v_cnt = 0 THEN
    insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,CONNECT_MODEL,ADDR_OTHER_NAME,PORTS,addr_type,cover_scenario)
    select cur_r.cityId,cur_r.houseId,cur_r.houseId,9,cur_r.houseNm,cur_r.floorId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId||'-'||cur_r.buildingId||'-'||cur_r.unitId||'-'||cur_r.floorId||'-'||cur_r.houseId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.coverType,cur_r.fullAddr,cur_r.PORTS,cur_r.addr_type,cur_r.cover_scenario
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.houseId and region=cur_r.cityId) and cur_r.houseId is not null;
    dbms_output.put_line('-->当v_cnt=0时，表 im_pres_stdaddress 表中相应的数据插入成功');
   else
     delete from im_pres_stdaddress where region=i_region  and addr_id = cur_r.houseId ;
   dbms_output.put_line('-->当v_cnt>0时，表 im_pres_stdaddress 中相应的数据删除成功');
     insert into im_pres_stdaddress(region,addr_id,org_addr_id,addr_level,ADDR_NAME,parent_id,std_addr_id,status,CREATE_DATE,oper_id,status_date,BELONG_FILIALE,CONNECT_MODEL,ADDR_OTHER_NAME,PORTS,addr_type,cover_scenario)
    select cur_r.cityId,cur_r.houseId,cur_r.houseId,9,cur_r.houseNm,cur_r.floorId,cur_r.cityId||'-'||cur_r.countyId||'-'||cur_r.townId||'-'||cur_r.roadId||'-'||cur_r.zoneId||'-'||cur_r.buildingId||'-'||cur_r.unitId||'-'||cur_r.floorId||'-'||cur_r.houseId,'VALID',SYSDATE,'101',SYSDATE,'999',cur_r.coverType,cur_r.fullAddr,cur_r.PORTS,cur_r.addr_type,cur_r.cover_scenario
     from dual where  not exists (select 1 from im_pres_stdaddress b where addr_id = cur_r.houseId and region=cur_r.cityId) and cur_r.houseId is not null;
     dbms_output.put_line('-->当v_cnt>0时，表 im_pres_stdaddress 中相应的数据添加成功');
   end if;
   commit;
    EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    end;
end loop;
commit;
end loc_pres_stdaddress;
/


CREATE OR REPLACE PROCEDURE "P_LOC_REC_ENCARD_STAT"
is
--部署在资源库上
begin
    --补充卡归属地市，先关联已售卡的归属地市，对于未关联到的再按照未售数据关联，卡归属仍为空的数据，无法进行结算统计。
    update LOC_REC_ENCARD_MONTH_ZY t
       set t.card_region = (select a.region
                              from im_inv_reinforce_use a
                             where a.inv_id = t.vccardnum
                               and rownum = 1)
     where t.card_region is null;

    update LOC_REC_ENCARD_MONTH_ZY t
       set t.card_region = (select a.region
                              from im_inv_reinforce a
                             where a.inv_id = t.vccardnum
                               and rownum = 1)
     where t.card_region is null;

    --初始化新增帐期各地市结算数据为0：LOC_REC_ENCARD_ZY
    for cur in (select region from region_list where region<>'999') loop
    dbms_output.put_line('-->cur：region='||cur.region);
        insert into LOC_REC_ENCARD_ZY(REGION,CYCLE,FEEIN,FEEOUT)
        values(cur.region,to_char(add_months(sysdate,-1),'yyyymm'),0,0);
    dbms_output.put_line('-->表 LOC_REC_ENCARD_ZY 中相应的数据添加成功');
    end loop;

    --更新LOC_REC_ENCARD_ZY表数据。
       update LOC_REC_ENCARD_ZY a
          set a.feein  = (select nvl(sum(t.totalfee), 0)
                            from LOC_REC_ENCARD_MONTH_ZY t
                           where t.region <> t.card_region
                             and t.card_region is not null
                             and t.region = a.region),
              a.feeout = (select nvl(sum(t.totalfee), 0)
                            from LOC_REC_ENCARD_MONTH_ZY t
                           where t.region <> t.card_region
                             and t.card_region is not null
                             and t.card_region = a.region)
        where a.cycle = to_char(add_months(sysdate,-1),'yyyymm');
        commit;
exception
   when others then
   rollback;
end p_loc_rec_encard_stat;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_FLOW_CHECK_RES" (orgId  varchar2,
                                                  flowId varchar2,
                                                  dynamicTableName varchar2)
 IS
  entityInvIdIsExisted number(1); --库存信息表中的串号是否在实体表中存在
  invIdOrgId           varchar2(32); --库存信息表中串号单位
  invIdBusiStatus      varchar2(32); --库存信息表中串号状态
  --add begin hWX490842 2018-11-14 R005C20L86AA5 DTS2018111603226
  displayName          varchar2(234); -- 组织机构展示名：组织机构名（组织机构ID）
  busiStatusName       varchar2(64); --库存信息表中串号状态翻译
  --add end hWX490842 2018-11-14 R005C20L86AA5 DTS2018111603226
  invIdStatusDate      date; --库存信息表中状态时间
  invResTypeId       varchar2(32); --资源类型
  diffCount            number(5) := 0; --差异数量
  invIdCount           number(5) := 0; --账本数量

  dynamicSQL varchar2(1000):=''; --动态sql

  type cur_type is ref cursor; --声明游标
  invCur cur_type; --定义游标变量
  invIdCur varchar2(32); --游标中的串号
begin
  --更新状态为“待异步稽核”盘点单的状态为“处理中”
  UPDATE PSI_FLOW_CHECK T
     SET T.FLOW_STATUS = 'PROCESSING'
   WHERE T.FLOW_ID = flowId
   AND T.FLOW_STATUS = 'UNAUDITED';
   --不处理无序资源
   if dynamicTableName = 'im_inv_unsign' then
     dbms_output.put_line('-->当dynamicTableName值为im_inv_unsign时程序直接跳转到末尾然后退出');
     goto unDealUnsign;
   end if;
   
  --1.根据盘点单编号查询出批次信息，按照批次来进行盘点核对
  for FLOWCHECKBATCHINFO IN (select t1.region,
                                    t1.flow_id,
                                    t1.res_kind_id,
                                    t1.org_id,
                                    t1.status_date,
                                    t1.start_date,
                                    t1.end_date,
                                    t1.check_date,
                                    t1.settle_mode, -- add by hWX490842 2018-12-29 R005C20L86AA7 OR_SD_201812_32 查询供货模式
                                    t2.batch_id,
                                    t2.res_type_id,
                                    t2.stock_num,
                                    t2.entity_num,
                                    t2.diff_num
                               from PSI_FLOW_CHECK        t1,
                                    PSI_FLOW_CHECK_DETAIL t2
                              where T1.FLOW_ID = T2.FLOW_ID
                                AND T1.FLOW_STATUS = 'PROCESSING'
                                AND t1.flow_id = flowId)
    LOOP
    --loop1
    begin
    dbms_output.put_line('-->1：该处有数据');
    dbms_output.put_line('-->1：org_id=' || FLOWCHECKBATCHINFO.org_id);
    dbms_output.put_line('-->1：res_type_id=' || FLOWCHECKBATCHINFO.res_type_id);
    dbms_output.put_line('-->1：settle_mode=' || FLOWCHECKBATCHINFO.settle_mode);
    dbms_output.put_line('-->1：batch_id=' || FLOWCHECKBATCHINFO.batch_id);
    dbms_output.put_line('-->1：flow_id=' || FLOWCHECKBATCHINFO.flow_id);
      --FLOWCHECKBATCHINFO begin
      --dbms_output.put_line('===='||FLOWCHECKBATCHINFO.batch_id);
      --2.以单位库存信息进行核对（表：im_inv_mobtel，条件：单位、状态：可用）
      --拼写动态sql
      -- modify begin hWX490842 2018-12-29 R005C20L86AA7 OR_SD_201812_32 按供货模式筛选串码
      -- 供货模式为全部（allSettle）时不筛选供货模式
      --modify begin hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表,考虑KEEP状态
      if (FLOWCHECKBATCHINFO.Settle_mode = 'allSettle') then
       dbms_output.put_line('-->1：当FLOWCHECKBATCHINFO.Settle_mode = allSettle时打印');
         dynamicSQL:= 'select t1.inv_id'||
                    ' from '||dynamicTableName||' t1 '||
                    ' where t1.org_id = '''||FLOWCHECKBATCHINFO.Org_Id||''' '||
                    ' and t1.BUSI_STATUS IN (''USABLE'',''KEEP'')'||
                    ' and t1.RES_TYPE_ID = '''||FLOWCHECKBATCHINFO.Res_Type_Id||''''||
                    ' UNION ALL'||
                    ' select t2.inv_id'||
                    ' from im_inv_mobtel_keep t2'||
                    ' where t2.org_id = '''||FLOWCHECKBATCHINFO.Org_Id||''' '||
                    ' and t2.BUSI_STATUS IN (''USABLE'',''KEEP'')'||
                    ' and t2.RES_TYPE_ID = '''||FLOWCHECKBATCHINFO.Res_Type_Id||'''';
    dbms_output.put_line('-->1：当FLOWCHECKBATCHINFO.Settle_mode = allSettle时dynamicSQL='||dynamicSQL);
      ELSE
     dbms_output.put_line('-->1：当FLOWCHECKBATCHINFO.Settle_mode != allSettle时打印');
         dynamicSQL:= 'select t1.inv_id'||
                    ' from '||dynamicTableName||' t1 '||
                    ' where t1.org_id = '''||FLOWCHECKBATCHINFO.Org_Id||''' '||
                    ' and t1.BUSI_STATUS IN (''USABLE'',''KEEP'')'||
                    ' and t1.SETTLE_MODE = '''||FLOWCHECKBATCHINFO.Settle_mode||''' '||
                    ' and t1.RES_TYPE_ID = '''||FLOWCHECKBATCHINFO.Res_Type_Id||''''||
                    ' UNION ALL'||
                    ' select t2.inv_id'||
                    ' from im_inv_mobtel_keep t2'||
                    ' where t2.org_id = '''||FLOWCHECKBATCHINFO.Org_Id||''' '||
                    ' and t2.BUSI_STATUS IN (''USABLE'',''KEEP'')'||
                    ' and t2.SETTLE_MODE = '''||FLOWCHECKBATCHINFO.Settle_mode||''' '||
                    ' and t2.RES_TYPE_ID = '''||FLOWCHECKBATCHINFO.Res_Type_Id||'''';

      end if;
      --modify end hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表
      -- modify end hWX490842 2018-12-29 R005C20L86AA7 OR_SD_201812_32 按供货模式筛选串码
      open invCur for dynamicSQL; --打开游标
      fetch invCur into invIdCur; --读取数据
    dbms_output.put_line('-->1：打开游标读取数据，invIdCur=' || invIdCur);
      while invCur%found
      loop  --loop 游标
          begin
            --invIdCur begin
            select count(1)
              into entityInvIdIsExisted
              from PSI_FLOW_CHECK_ENTITY t
             where t.inv_id = invIdCur
           and t.flow_id  = flowId
         and t.batch_id = FLOWCHECKBATCHINFO.batch_id;
     dbms_output.put_line('-->1：while循环中值entityInvIdIsExisted='||entityInvIdIsExisted);
            if (entityInvIdIsExisted = 0) then
              --a.串号不在实体表（PSI_FLOW_CHECK_ENTITY）中，处理结果:插入记录，账本在库、实物不在库，核对状态为完毕，
              --核对结果为“实物不存在”，差异结果设置为异常
              INSERT INTO PSI_FLOW_CHECK_ENTITY
                (REGION,
                 INV_ID,
                 RES_TYPE_ID,
                 BATCH_ID,
                 FLOW_ID,
                 IS_EXIST_STOCK,
                 IS_EXIST_ENTITY,
                 CREATE_DATE,
                 CHECK_STATUS,
                 CHECK_DATE,
                 CHECK_RESULT,
                 CHECK_MEMO,
                 DIFF_RESULT,
                 DIFF_DATE,
                 INV_ORG_ID)
              values
                (FLOWCHECKBATCHINFO.region,
                 invIdCur,
                 FLOWCHECKBATCHINFO.Res_Type_Id,
                 FLOWCHECKBATCHINFO.Batch_Id,
                 FLOWCHECKBATCHINFO.Flow_Id,
                 1, --账本在库
                 0, --实物不在库
                 SYSDATE,
                 'SUCC',
                 SYSDATE,
                 'ENTITY_UNEXISTED',
                 '实物不存在',
                 'EXCEPT',
                 SYSDATE,
                 FLOWCHECKBATCHINFO.Org_Id);
              --差异数量加1
              -- add begin hWX490842 2018-12-29 R005C20L86AA7 OR_SD_201812_32
              --账本数量
              invIdCount := invIdCount+1;
              -- add end hWX490842 2018-12-29 R005C20L86AA7 OR_SD_201812_32
              diffCount := diffCount+1;
            elsif (entityInvIdIsExisted = 1) then
             --add begin hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 实物存在查 账本串码状态，状态为KEEP单独记录盘点结果
             --为 '账实相符且状态为KEEP' ，其他记录为'账实相符'
            dynamicSQL:= 'select T.BUSI_STATUS'||
                         ' from v_inv_mobtel t '||
                         ' where t.inv_id = '''||invIdCur||''' '||
                         ' UNION ALL'||
                         ' select T2.BUSI_STATUS'||
                         ' from im_inv_mobtel_keep T2'||
                         ' where T2.inv_id = '''||invIdCur||''' ';
            execute immediate dynamicSQL into invIdBusiStatus;
      dbms_output.put_line('-->1：动态SQL中的invIdBusiStatus值='||invIdBusiStatus);
            --实物在库、账本在库，账本串码状态为KEEP,平盘，记录'账实相符且状态为KEEP'
            IF (invIdBusiStatus = 'KEEP') THEN
               UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '1', --账本在库
                     T.IS_EXIST_ENTITY = '1', --实物在库
                     T.CHECK_STATUS    = 'SUCC', --核对完毕
                     T.CHECK_DATE      = SYSDATE, --核对时间
                     T.CHECK_RESULT    = 'STOCK_ENTITY_ACCORD', --账实相符
                     T.CHECK_MEMO      = '账实相符且状态为:库龄超期冻结(KEEP)',
                     T.DIFF_RESULT     = 'NORMAL', --差异结果：正常
                     T.DIFF_DATE       = SYSDATE
               WHERE T.INV_ORG_ID = FLOWCHECKBATCHINFO.Org_Id
                 AND T.FLOW_ID = flowId
                 AND T.BATCH_ID = FLOWCHECKBATCHINFO.Batch_Id
                 AND T.CHECK_STATUS = 'PEND'
                 AND T.INV_ID = invIdCur;
                 --账本数量
                 invIdCount := invIdCount+1;
         dbms_output.put_line('-->1：表 PSI_FLOW_CHECK_ENTITY 数据更新成功');
         dbms_output.put_line('-->1：账本数量值invIdCount='||invIdCount);
             ELSE
            --add end hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表
              -- dbms_output.put_line('11111=='||FLOWCHECKBATCHINFO.Batch_Id);
              --串号在实体表，处理结果：标识为账本在库、实物在库，核对状态为完毕，核对结果为“帐实相符”，
              --差异结果设置为正常.
              dbms_output.put_line('1==' || invIdCur);
              UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '1', --账本在库
                     T.IS_EXIST_ENTITY = '1', --实物在库
                     T.CHECK_STATUS    = 'SUCC', --核对完毕
                     T.CHECK_DATE      = SYSDATE, --核对时间
                     T.CHECK_RESULT    = 'STOCK_ENTITY_ACCORD', --账实相符
                     T.CHECK_MEMO      = '账实相符',
                     T.DIFF_RESULT     = 'NORMAL', --差异结果：正常
                     T.DIFF_DATE       = SYSDATE
               WHERE T.INV_ORG_ID = FLOWCHECKBATCHINFO.Org_Id
                 AND T.FLOW_ID = flowId
                 AND T.BATCH_ID = FLOWCHECKBATCHINFO.Batch_Id
                 AND T.CHECK_STATUS = 'PEND'
                 AND T.INV_ID = invIdCur;
                 --账本数量
                 invIdCount := invIdCount+1;
               end if;
            end if;
          end; ----invIdCur end
        fetch invCur into invIdCur; --读取数据
      end loop; --loop 游标
      close invCur; --关闭游标

      --2.以串号实体表进行核对（表:PSI_FLOW_CHECK_ENTITY,条件：串号，状态:待处理）
    dbms_output.put_line('-->2：以串号实体表进行核对');
      for FLOWCHECKENTITY in (select t.inv_id, t.inv_org_id
                                   from PSI_FLOW_CHECK_ENTITY t
                               where t.FLOW_ID = flowId
                                 and t.batch_id =
                                     FLOWCHECKBATCHINFO.Batch_Id
                                 and t.inv_org_id =
                                     FLOWCHECKBATCHINFO.Org_Id
                                 and t.check_status = 'PEND') loop
        --loop3
        begin
      dbms_output.put_line('-->2：inv_id='|| FLOWCHECKENTITY.inv_id);
      dbms_output.put_line('-->2：inv_org_id='|| FLOWCHECKENTITY.inv_org_id);
          --modify begin hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表
          dynamicSQL:= 'select ss1+ss2 from'||
                       '(select count(1) as ss1 from v_inv_mobtel t where t.inv_id = '''||FLOWCHECKENTITY.inv_id||''''||
                       ')'||
                       ' s1 ,'||
                       '(select count(1) as ss2 from im_inv_mobtel_keep t where t.inv_id = '''||FLOWCHECKENTITY.inv_id||''''||
                       ') s2';
          --modify end hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3
          execute immediate dynamicSQL into entityInvIdIsExisted; --动态执行sql
      dbms_output.put_line('-->2：动态SQL：entityInvIdIsExisted='||entityInvIdIsExisted);
          --串号不在库存信息表中（表:im_inv_mobtel）,处理结果：标识为账本不在库、
          --实物在库，核对状态为完毕，核对结果为“账本不在库”，差异结果设置为异常
          if (entityInvIdIsExisted = 0) then
        dbms_output.put_line('-->2：当entityInvIdIsExisted=0时值entityInvIdIsExisted=' || entityInvIdIsExisted);
            --在串号表中不存在
            UPDATE PSI_FLOW_CHECK_ENTITY T
               SET T.IS_EXIST_STOCK  = '0',
                   T.IS_EXIST_ENTITY = '1',
                   T.CHECK_STATUS    = 'SUCC',
                   T.CHECK_DATE      = SYSDATE,
                   T.CHECK_RESULT    = 'STOCK_UNEXISTED',--账本不在库
                   T.CHECK_MEMO      = '账本不在库',
                   T.DIFF_RESULT     = 'EXCEPT',
                   T.DIFF_DATE       = SYSDATE
             WHERE t.FLOW_ID = flowId
               and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
               and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
               and t.inv_id = FLOWCHECKENTITY.inv_id
               and t.check_status = 'PEND';
               --差异数量加1
               diffCount := diffCount+1;
         dbms_output.put_line('-->2：差异数量diffCount='||diffCount);
         dbms_output.put_line('-->2：表 PSI_FLOW_CHECK_ENTITY 中的数据更新成功');
            --串号存在库存信息表中（表:im_inv_mobtel）
          elsif (entityInvIdIsExisted = 1) then
            --dbms_output.put_line('1==' || FLOWCHECKENTITY.inv_id);
            --查询串号在库存信息表中基本信息(串号状态、串号时间)

            -- modify begin hWX490842 2018-11-14 DTS2018111603226 查询业务状态翻译值
            --modify begin hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表
            dynamicSQL:= 'select T.ORG_ID,'||
                         '(SELECT C.DISPLAYNAME FROM PSI_ORGANIZATION C WHERE C.ORGID = T.ORG_ID) DISNAME,'||
                         'T.BUSI_STATUS, T.STATUS_DATE ,T.RES_TYPE_ID,'||
                         '(SELECT A.DICT_NAME FROM PSI_DICT_ITEM A WHERE A.GROUP_ID=''BUSI_STATUS'' AND A.DICT_ID=T.BUSI_STATUS)  statusName'||
                         ' from v_inv_mobtel t '||
                         ' where t.inv_id = '''||FLOWCHECKENTITY.inv_id||''' '||
                         ' UNION ALL'||
                         ' select T2.ORG_ID,'||
                         '(SELECT C.DISPLAYNAME FROM PSI_ORGANIZATION C WHERE C.ORGID = T2.ORG_ID) DISNAME,'||
                         ' T2.BUSI_STATUS, T2.STATUS_DATE ,T2.RES_TYPE_ID,'||
                         '(SELECT B.DICT_NAME FROM PSI_DICT_ITEM B WHERE B.GROUP_ID=''BUSI_STATUS'' AND B.DICT_ID=T2.BUSI_STATUS) as statusName'||
                         ' from im_inv_mobtel_keep T2'||
                         ' where T2.inv_id = '''||FLOWCHECKENTITY.inv_id||''' ';
            --modify end hWX490842 2018-8-2 R005C20L86AA2 OR_SD_201807_3 核对追加考虑im_inv_mobtel_keep表
            -- modify end hWX490842 2018-11-14 DTS2018111603226
            execute immediate dynamicSQL into invIdOrgId,displayName,invIdBusiStatus,invIdStatusDate ,invResTypeId,busiStatusName;

            --单位相同，且状态为已售，处理结果:标识为账本不在库、实物在库，
            --核对状态为完毕，核对结果为“已经售出”, 如果销售时间大于盘点时间，处理结果为正常，
            --否则为异常
            if (invIdOrgId = orgId AND invIdBusiStatus = 'USED' AND
        FLOWCHECKBATCHINFO.res_type_id = invResTypeId AND
               invIdStatusDate >= FLOWCHECKBATCHINFO.Start_Date) THEN
              --dbms_output.put_line('已销售，销售时间大于盘点时间，属于正常状态');
              UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '0',
                     T.IS_EXIST_ENTITY = '1',
                     T.CHECK_STATUS    = 'SUCC',
                     T.CHECK_DATE      = SYSDATE,
                     T.CHECK_RESULT    = 'SELLED',--已售出
                     T.CHECK_MEMO      = '销售时间为' ||
                                         TO_CHAR(invIdStatusDate,
                                                 'YYYY-MM-DD hh24:mi:ss'),
                     T.DIFF_RESULT     = 'NORMAL',
                     T.DIFF_DATE       = SYSDATE
               WHERE t.FLOW_ID = flowId
                 and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
                 and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
                 and t.inv_id = FLOWCHECKENTITY.inv_id
                 and t.check_status = 'PEND';
                 --账本数量
                 invIdCount := invIdCount+1;
            ELSIF (invIdOrgId = orgId AND invIdBusiStatus = 'USED' AND
          FLOWCHECKBATCHINFO.res_type_id = invResTypeId AND
                  invIdStatusDate < FLOWCHECKBATCHINFO.Start_Date) then
              --dbms_output.put_line('已销售，销售时间小于于盘点时间，属于异常状态');
              UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '0',
                     T.IS_EXIST_ENTITY = '1',
                     T.CHECK_STATUS    = 'SUCC',
                     T.CHECK_DATE      = SYSDATE,
                     T.CHECK_RESULT    = 'SELLED_TIME_EXCEPT',--已售出时间异常
                     T.CHECK_MEMO      = '销售时间为' ||
                                         TO_CHAR(invIdStatusDate,
                                                 'YYYY-MM-DD hh24:mi:ss'),
                     T.DIFF_RESULT     = 'EXCEPT',
                     T.DIFF_DATE       = SYSDATE
               WHERE t.FLOW_ID = flowId
                 and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
                 and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
                 and t.inv_id = FLOWCHECKENTITY.inv_id
                 and t.check_status = 'PEND';
                 --差异数量加1
                 diffCount := diffCount+1;
            ELSIF (invIdOrgId = orgId AND FLOWCHECKBATCHINFO.res_type_id = invResTypeId ) then
              --dbms_output.put_line('状态不符');
              -- modify begin hWX490842 2018-11-14 展示状态翻译值：busiStatusName代替invIdBusiStatus
              UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '0',
                     T.IS_EXIST_ENTITY = '1',
                     T.CHECK_STATUS    = 'SUCC',
                     T.CHECK_DATE      = SYSDATE,
                     T.CHECK_RESULT    = 'STATUS_UNACCORD',--状态不符
                     T.CHECK_MEMO      = 'imei在库存中状态为:' || busiStatusName ||'('|| invIdBusiStatus ||')',
                     T.DIFF_RESULT     = 'EXCEPT',
                     T.DIFF_DATE       = SYSDATE
               WHERE t.FLOW_ID = flowId
                 and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
                 and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
                 and t.inv_id = FLOWCHECKENTITY.inv_id
                 and t.check_status = 'PEND';
                 --差异数量加1
                 diffCount := diffCount+1;
            elsif (invIdOrgId != orgId AND FLOWCHECKBATCHINFO.res_type_id = invResTypeId) then
              UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '0',
                     T.IS_EXIST_ENTITY = '1',
                     T.CHECK_STATUS    = 'SUCC',
                     T.CHECK_DATE      = SYSDATE,
                     T.CHECK_RESULT    = 'ORG_UNACCORD',--单位不符
                     T.CHECK_MEMO      = 'imei在库存中状态为:' || busiStatusName ||'('|| invIdBusiStatus ||')'||
                                         '，单位为:' || displayName,
                     T.DIFF_RESULT     = 'EXCEPT',
                     T.DIFF_DATE       = SYSDATE
               WHERE t.FLOW_ID = flowId
                 and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
                 and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
                 and t.inv_id = FLOWCHECKENTITY.inv_id
                 and t.check_status = 'PEND';
                 -- modify end hWX490842 2018-11-14
                 --差异数量加1
                 diffCount := diffCount+1;
      elsif( FLOWCHECKBATCHINFO.res_type_id != invResTypeId ) then
             UPDATE PSI_FLOW_CHECK_ENTITY T
                 SET T.IS_EXIST_STOCK  = '0',
                     T.IS_EXIST_ENTITY = '1',
                     T.CHECK_STATUS    = 'SUCC',
                     T.CHECK_DATE      = SYSDATE,
                     T.CHECK_RESULT    = 'RES_UNACCORD',--资源类型不符
                     T.CHECK_MEMO      = 'imei在库存中资源类型为:' || invResTypeId ||
                                         '，实物资源类型为:' || FLOWCHECKBATCHINFO.res_type_id,
                     T.DIFF_RESULT     = 'EXCEPT',
                     T.DIFF_DATE       = SYSDATE
               WHERE t.FLOW_ID = flowId
                 and t.batch_id = FLOWCHECKBATCHINFO.Batch_Id
                 and t.inv_org_id = FLOWCHECKBATCHINFO.Org_Id
                 and t.inv_id = FLOWCHECKENTITY.inv_id
                 and t.check_status = 'PEND';
                 --差异数量加1
                 diffCount := diffCount+1;
            end if;
          end if;
        end;
      end loop; --loop3
    end; --FLOWCHECKBATCHINFO end
    --统计每一批次的差异数量、账本数量
    update PSI_FLOW_CHECK_DETAIL t
       SET T.STOCK_NUM = invIdCount , T.DIFF_NUM = diffCount
     where t.flow_id = FLOWCHECKBATCHINFO.FLOW_ID
       and t.batch_id = FLOWCHECKBATCHINFO.BATCH_ID;
     --核对下一盘点批次前，重置差异数量、账本数量 add by hWX490842 20180824 R005C20L86AA2 OR_SD_201807_3
   dbms_output.put_line('-->赋值前：diffCount=' || diffCount);
   dbms_output.put_line('-->赋值前：invIdCount=' || invIdCount);
     diffCount    := 0; --差异数量
     invIdCount   := 0; --账本数量
   dbms_output.put_line('-->赋值后：diffCount=' || diffCount);
   dbms_output.put_line('-->赋值后：invIdCount=' || invIdCount);
    end loop; --loop1
  --无序资源直接结束
  <<unDealUnsign>>
  --事务提交
  COMMIT;
  --事务回滚
EXCEPTION
  WHEN OTHERS THEN
    BEGIN
        dbms_output.put_line('===orgid===' ||sqlerrm);
      rollback;
    end;
end;
/


CREATE OR REPLACE PROCEDURE "PSI_PROC_EXPIREDORDERCANCEL" is
  v_procId varchar2(32);
begin

  for regionList in (select t.region from region_list t) loop
    begin
      for orderList in (select t.region,
                               t.flow_id,
                               t.apply_type,
                               t.res_kind_id
                          from psi_flow_apply t
                         where t.region = regionList.Region
                           and t.apply_type in
                               ('CATA_MOBAPPLY_AGENTSELL',
                                'CATA_MOBAPPLY_AGENTPRESELL')
                           and t.create_date > sysdate - 10
                           and t.flow_status = 'UNPAYMENT'
                           and t.pay_status = 'PEND'
                           and (sysdate - t.status_date) * 24 >=
                               (select m.param_value
                                  from psi_param_value m
                                 where m.param_id =
                                       'EXPIRED_UNPAY_CANCEL_HOURS')) loop
        begin
     dbms_output.put_line('-->regionList：region='||regionList.region);
     dbms_output.put_line('-->orderList：region='||orderList.region);
     dbms_output.put_line('-->orderList：flow_id='||orderList.flow_id);
     dbms_output.put_line('-->orderList：apply_type='||orderList.apply_type);
     dbms_output.put_line('-->orderList：res_kind_id='||orderList.res_kind_id);
          update psi_flow_apply t
             set t.flow_status = 'cancel', t.status_date = sysdate
           where t.flow_id = orderList.Flow_Id
             and t.region = orderList.Region;
          SELECT TO_CHAR(SYSDATE, 'YYMMDDHH24MI') ||
                 LPAD(TO_CHAR(SEQ_BUSI_ID.NEXTVAL), 14, '0')
            into v_procId
            FROM DUAL;
          dbms_output.put_line('-->v_procId的值='||v_procId);
          insert into psi_flow_proc_log (REGION, PROC_ID, FLOW_ID, FLOW_TYPE, OPER_TYPE, RES_KIND_ID, ORG_ID, ROLE_ID, PROC_RESULT, PROC_OPER, IS_ATTACHMENT, CREATE_DATE, MEMO)
          values
            (orderList.Region,
             v_procId,
             orderList.Flow_Id,
             orderList.Apply_Type,
             'PSI_CANCEL',
             orderList.Res_Kind_Id,
             'SD',
             'sysRole',
             'cancel',
             'sysOper',
             'NO',
             sysdate,
             '超期未支付，自动取消');
      dbms_output.put_line('-->表 psi_flow_proc_log 数据插入成功');
        end;
      end loop;
    end;
  end loop;
  commit;
end psi_proc_expiredordercancel;
/

CREATE OR REPLACE PROCEDURE "AP_IM_APPLY_LIMITSTAT" (IREGION    IN VARCHAR2)
 as
  v_Date DATE;
begin
  select to_date('20201015','yyyy-mm-dd') into v_Date from dual;
  delete from IM_APPLY_LIMIT_STAT;
  dbms_output.put_line('-->v_Date的值为'||v_Date);
  --统计当前全县未售出号码量
  insert into IM_APPLY_LIMIT_STAT(region,area_id,segment_id,stat_kind,total_num,create_date)
    select a.region, b.areaid, substr(a.inv_id,1,7), 'TELNUM_STAT', count(*), v_Date
      FROM IM_INV_TELNUM a, ORGANIZATION b
     where a.REGION = b.REGION
       and a.REGION = iRegion
       and a.ORG_ID = b.ORGID
       and a.BUSI_STATUS in ('INACTIVE', 'USABLE')
  group by a.region, b.areaid, substr(a.inv_id,1,7);

  --统计当前全县未号段展开量
  insert into IM_APPLY_LIMIT_STAT(region,area_id,segment_id,stat_kind,total_num,create_date)
    select a.region,b.Areaid,a.segment_id,'SEGMENT_STAT',nvl(sum(a.USABLE_QUANTITY),0), v_Date
      FROM IM_RESOURCE_SEGMENT_SPLIT a, ORGANIZATION b
     where a.REGION = b.REGION
       and a.REGION = iRegion
       and a.ORG_ID = b.ORGID
       and a.RES_TYPE_ID = 'rsclTgsm'
  group by a.region,b.Areaid,a.segment_id;

  --统计未来3个月全县可回收号码量
  insert into IM_APPLY_LIMIT_STAT (region,area_id,segment_id,stat_kind,total_num,create_date)
    select a.region,b.Areaid,substr(a.inv_id,1,7),'LOGOUT_STAT',count(*), v_Date
      FROM IM_INV_TELNUM_USE a, ORGANIZATION b
     where a.REGION = b.REGION
       and a.REGION = iRegion
       and a.ORG_ID = b.ORGID
       and a.BUSI_STATUS = 'LOGOUT'
       and a.STATUS_DATE > SYSDATE - 90
  group by a.region, b.areaid, substr(a.inv_id,1,7);

  --统计当前全县SIM库存余量
  insert into IM_APPLY_LIMIT_STAT(region,area_id,segment_id,stat_kind,total_num,create_date)
    select a.REGION,b.Areaid,a.STOCK_GROUP,'STOCK_STAT',nvl(sum(a.INCOME_AMOUNT - a.OUTGO_AMOUNT), 0),v_Date
      FROM IM_STK_SACCOUNT a, ORGANIZATION b
     where a.REGION = b.REGION
       and a.REGION = iRegion
       and a.ORG_ID = b.ORGID
       and a.RES_KIND_ID = 'rsclSgsm'
  group by a.region, b.areaid,a.STOCK_GROUP ;
  dbms_output.put_line('--表 IM_APPLY_LIMIT_STAT 中的数据添加成功');
  commit;
EXCEPTION
  WHEN OTHERS THEN
    begin
      ROLLBACK;
      raise_application_error(-1,
                              'INSERT table ERROR! SQLERRM=[' || SQLERRM || ']');
      return;
    end;
end;
/

CREATE OR REPLACE PROCEDURE "LOC_PRC_UNINTERVIEW_CHNLINFO" is
  --j             number(10);
  v_monthtercnt number(10);
  v_monthnumcnt number(10);
BEGIN
  --for i in 1 .. 7 LOOP
    DELETE FROM loc_uninterview_chnlinfo t ;
    --j := 1;
    for v_record in (select t.agentid,t.region
                       from agent t,ORGANIZATION p
                      WHERE p.ORGID=t.agentid AND p.STATUS=1
            AND t.agenttype IN ('300020','300021','300040','300041','300042','300060','300061')) loop
      dbms_output.put_line('-->v_record：agentid='||v_record.agentid);
      dbms_output.put_line('-->v_record：region='||v_record.region);
    ---当月业务量信息(终端)
      select SUM(T.SALE_NUM)
        into v_monthtercnt
        from psi_stat_daypsi t
       WHERE t.org_id = v_record.agentid
         and t.region=v_record.region
         AND to_date(t.stat_day,'yyyymmdd') > trunc(add_months(to_date('20201015','yyyy-mm-dd'), -1), 'mm')
         and to_date(t.stat_day,'yyyymmdd') < trunc(to_date('20201116','yyyy-mm-dd'), 'mm');
      --当月业务量信息(放号)
      select count(1)
        into v_monthnumcnt
        from im_inv_telnum t
       WHERE t.region=v_record.region
         and t.org_id = v_record.agentid
         AND t.create_date > trunc(add_months(to_date('20201015','yyyy-mm-dd'), -1), 'mm')
         and t.create_date < trunc(to_date('20201116','yyyy-mm-dd'), 'mm')
         and  t.inv_status = 'INSTORE'
         and t.busi_status = 'USABLE';
    dbms_output.put_line('-->v_monthtercnt='||v_monthtercnt);
    dbms_output.put_line('-->v_monthnumcnt='||v_monthnumcnt);
      insert into loc_uninterview_chnlinfo
        (AGENTID, MONTH, MONTHTERCNT, MONTHNUMCNT)
      values
        (v_record.agentid,
         to_char(add_months(sysdate, -1), 'yyyymm'),
         v_monthtercnt,
         v_monthnumcnt);
    dbms_output.put_line('-->表 loc_uninterview_chnlinfo 中相应的数据更新成功');
      ----每100条提交一次
      --j := j + 1;
      --if (mod(j, 100) = 0) then
        commit;
      --end if;
    end loop;
    commit;
  --end loop;
  --commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end loc_prc_uninterview_chnlinfo;
/

CREATE OR REPLACE PROCEDURE "ORGANIZATION_MOBILESHOP_INIT" AS
begin
  --delete all data existed in im_if_bb_MobileShop firstly
  delete from im_if_bb_MobileShop;
  commit;

  for cur_org_out in (select orgid, createdate, region, orgname
                        from organization
                        where status = 1 and orgtype = 'ogkdDept') loop
    begin
    dbms_output.put_line('-->cur_org_out循环：orgid='||cur_org_out.orgid);
    dbms_output.put_line('-->cur_org_out循环：createdate='||cur_org_out.createdate);
    dbms_output.put_line('-->cur_org_out循环：region='||cur_org_out.region);
    dbms_output.put_line('-->cur_org_out循环：orgname='||cur_org_out.orgname);
      INSERT INTO IM_IF_BB_MOBILESHOP
        (REGION, INTF_DATE, REC_SEQ,
         SHOPCODE, SHOPNAME,
         SHOPTYPE, CHANNELTYPE, EFFECTIVEDATE, CLOSEDATE,
         TRANSTYPE, STATUS, STATUS_DATE, MEMO)
      VALUES
        (cur_org_out.REGION, TO_CHAR(SYSDATE, 'YYYYMMDD'), TO_CHAR(SEQ_OID.NEXTVAL),
         cur_org_out.ORGID, cur_org_out.ORGNAME,
         2, 0, cur_org_out.createdate, TO_DATE('20381230', 'YYYYMMDD'),
         1, 'PEND', SYSDATE, '移动营业厅店数据初始化');
   dbms_output.put_line('-->cur_org_out循环：表中 IM_IF_BB_MOBILESHOP 相应的数据插入成功');
    end;
  end loop;
  commit;
end;
/

CREATE OR REPLACE PROCEDURE "P_LOC_SHOPJXHM_AA" (i_region number,i_tag number)
AS
  v_rec_count number(8);
  row_num number(8):=1;
  v_orgid varchar2(16);
  v_region number(5);
  v_flag number(1);
  v_low_fee_max number(10);
  v_low_pre_max number(10);
  v_low_time_max number(10);
  v_privid_max varchar2(32);
  v_patterntype varchar2(32);

  type telArry is table of varchar2(20) index by binary_integer;
  type typ_rec is record(telpattern varchar2(20),low_consum_fee number(10), low_consum_pre number(10)
                        ,low_inservice_time number(10),privid varchar2(32),patterntype varchar2(32));
  type typ_tab is table of typ_rec index by binary_integer;
  v_telnum     telArry;
  v_pattern    typ_tab;
BEGIN
  IF i_tag=1 THEN
     v_region:=i_region;
  ELSE
     v_region:=999;
  END IF;
  SELECT COUNT(*) INTO v_rec_count FROM Loc_cs_shopjxhmrule WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1;
  dbms_output.put_line('-->i_tag='||i_tag);
  dbms_output.put_line('-->i_region='||i_region);
  dbms_output.put_line('-->v_rec_count='||v_rec_count);
  --加载号码模式表的数据到数组中
  FOR cur_pat IN (SELECT TELPATTERN,LOW_CONSUM_FEE,LOW_CONSUM_PRE,LOW_INSERVICE_TIME,PRIVID,PATTERNTYPE FROM LOC_CS_SHOPJXHMRULE WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1)LOOP
      BEGIN
          v_pattern(row_num).telpattern:=cur_pat.Telpattern;
          v_pattern(row_num).low_consum_fee:=cur_pat.low_consum_fee;
          v_pattern(row_num).low_consum_pre:=cur_pat.low_consum_pre;
          v_pattern(row_num).low_inservice_time:=cur_pat.low_inservice_time;
          v_pattern(row_num).privid:=cur_pat.privid;
          v_pattern(row_num).patterntype:=cur_pat.patterntype;
          --DBMS_OUTPUT.put_line(v_pattern(row_num).telpattern);
          row_num:=row_num+1;
      dbms_output.put_line('-->cur_pat：telpattern='||cur_pat.telpattern);
      dbms_output.put_line('-->cur_pat：low_consum_fee='||cur_pat.low_consum_fee);
      dbms_output.put_line('-->cur_pat：low_consum_pre='||cur_pat.low_consum_pre);
      dbms_output.put_line('-->cur_pat：low_inservice_time='||cur_pat.low_inservice_time);
      dbms_output.put_line('-->cur_pat：privid='||cur_pat.privid);
      dbms_output.put_line('-->cur_pat：patterntype='||cur_pat.patterntype);
      END;
  END LOOP;

  --SELECT ORGID||'.ES' INTO v_orgid FROM region_list WHERE REGION=i_region;
  --dbms_output.put_line('-->v_orgid='||v_orgid);
  FOR cur_tel IN (SELECT REGION,INV_ID
                    FROM tmp_telnum_aa
                   WHERE REGION=i_region
                    ) LOOP
      BEGIN
      dbms_output.put_line('-->cur_tel：REGION='|| cur_tel.REGION);
      dbms_output.put_line('-->cur_tel：INV_ID='|| cur_tel.INV_ID);
        --循环依次取某一个号码的后8、7、6、5、4、3、2位，与号码模式表的每条数据相匹配
        v_flag:=0;
        v_low_fee_max:=-1;
        FOR I IN 1 .. 7 LOOP--取"最低消费"最高的模式
           v_telnum(I) := SUBSTR(cur_tel.INV_ID,I+3,9-I);
           FOR J IN 1 .. v_rec_count LOOP
           IF(v_pattern(J).telpattern=v_telnum(I)) THEN
              if(v_pattern(J).low_consum_fee>v_low_fee_max) then
                 v_low_fee_max:=v_pattern(J).low_consum_fee;
                 v_low_pre_max:=v_pattern(J).low_consum_pre;
                 v_low_time_max:=v_pattern(J).low_inservice_time;
                 v_privid_max:=v_pattern(J).privid;
                 v_patterntype:=v_pattern(J).patterntype;
         dbms_output.put_line('-->cur_tel循环：v_low_fee_max='||v_low_fee_max||' v_low_pre_max='||v_low_pre_max||' v_low_time_max='||v_low_time_max||' v_privid_max='||' v_patterntype='||v_patterntype);
              end if;
              v_flag:=1;
           END IF;
           END LOOP;
        END LOOP;
    dbms_output.put_line('-->v_flag='||v_flag);
        if v_flag=1 then
           --当号码匹配到一条规则且此号码是属于188号码时，当此规则的"最低消费"<188的"最低消费"时,取188的规则
           IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
              IF v_low_fee_max<5000 THEN
                 v_low_fee_max:=5000;
                 v_low_pre_max:=30000;
                 v_low_time_max:=5;
                 v_patterntype:='188';
                 v_privid_max:='gl.base.999649_24.500';
         dbms_output.put_line('-->当INV_ID截取后值为188且v_low_fee_max<5000时v_low_fee_max='||v_low_fee_max||
                  ' v_low_pre_max='||v_low_pre_max||
                  ' v_low_time_max='||v_low_time_max||
                  ' v_patterntype='||v_patterntype||
                  ' v_privid_max='||v_privid_max);
              END IF;
           END IF;
           update im_inv_telnum set low_consum_fee=v_low_fee_max,
                                    low_consum_pre=v_low_pre_max,
                                    low_inservice_time=v_low_time_max,
                                    memo=v_patterntype
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id
             and busi_status in('PICK','USABLE');
           --删除 IM_INV_APPEND表中重复的数据
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';

           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID',v_privid_max,sysdate);
           goto continue;
        end if;
        --匹配188普通号源
        IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
           update im_inv_telnum set low_consum_fee=5000,
                                    low_consum_pre=30000,
                                    low_inservice_time=5,
                                    memo='188'
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --删除 IM_INV_APPEND表中重复的数据
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';
           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID','gl.base.999649_24.500',sysdate);
           goto continue;
        END IF;
        --未匹配到号码入网预交为50
        update im_inv_telnum set low_consum_fee=0,low_consum_pre=5000,low_inservice_time=NULL
         Where region=cur_tel.region
           and INV_ID=cur_tel.inv_id
            and busi_status in('PICK','USABLE');
        <<continue>>
        null;
      END;
  END LOOP;
commit;
EXCEPTION
  WHEN OTHERS THEN
  rollback;
END P_LOC_SHOPJXHM_AA;
/

CREATE OR REPLACE PROCEDURE "P_LOC_SHOPJXHM" (i_region number,i_tag number)
AS
  v_rec_count number(8);
  row_num number(8):=1;
  v_orgid varchar2(16);
  v_region number(5);
  v_flag number(1);
  v_low_fee_max number(10);
  v_low_pre_max number(10);
  v_low_time_max number(10);
  v_privid_max varchar2(32);
  v_patterntype varchar2(32);
  type telArry is table of varchar2(20) index by binary_integer;
  type typ_rec is record(telpattern varchar2(20),low_consum_fee number(10), low_consum_pre number(10)
                        ,low_inservice_time number(10),privid varchar2(32),patterntype varchar2(32));
  type typ_tab is table of typ_rec index by binary_integer;
  v_telnum     telArry;
  v_pattern    typ_tab;
BEGIN
  IF i_tag=1 THEN
     v_region:=i_region;
  ELSE
     v_region:=999;
  END IF;
  SELECT COUNT(1) INTO v_rec_count FROM Loc_cs_shopjxhmrule WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1;
  dbms_output.put_line('-->v_rec_count='||v_rec_count);
  --加载号码模式表的数据到数组中
  FOR cur_pat IN (SELECT TELPATTERN,LOW_CONSUM_FEE,LOW_CONSUM_PRE,LOW_INSERVICE_TIME,PRIVID,PATTERNTYPE FROM LOC_CS_SHOPJXHMRULE WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1)LOOP
      BEGIN
          v_pattern(row_num).telpattern:=cur_pat.Telpattern;
          v_pattern(row_num).low_consum_fee:=cur_pat.low_consum_fee;
          v_pattern(row_num).low_consum_pre:=cur_pat.low_consum_pre;
          v_pattern(row_num).low_inservice_time:=cur_pat.low_inservice_time;
          v_pattern(row_num).privid:=cur_pat.privid;
          v_pattern(row_num).patterntype:=cur_pat.patterntype;
          --DBMS_OUTPUT.put_line(v_pattern(row_num).telpattern);
          row_num:=row_num+1;
      dbms_output.put_line('-->cur_pat：telpattern='||cur_pat.telpattern||
                 ' low_consum_fee='||cur_pat.low_consum_fee||
                 ' low_consum_pre='||cur_pat.low_consum_pre||
                 ' low_inservice_time='||cur_pat.low_inservice_time||
                 ' privid='||cur_pat.privid||
                 ' patterntype='||cur_pat.patterntype);
      dbms_output.put_line('-->row_num='||row_num);
      END;
  END LOOP;

  SELECT ORGID||'.ES' INTO v_orgid FROM region_list WHERE REGION=i_region;
  dbms_output.put_line('-->v_orgid='||v_orgid);
  FOR cur_tel IN (SELECT REGION,INV_ID,PROCESSED,PROCESSDATE,PROCESSINFO
                    FROM IM_ESHOP_TELNUM_INLOG
                   WHERE REGION=i_region
                     AND INTIME>=trunc(sysdate-3)  --时间
                     AND STATUS ='1'
                     --and is_sync = 0
                     AND PROCESSED = '0' for update) LOOP
      BEGIN
       dbms_output.put_line('-->cur_tel：REGION='||cur_tel.REGION||
                     ' INV_ID='||cur_tel.INV_ID||
                     ' PROCESSED='||cur_tel.PROCESSED||
                     ' PROCESSDATE='||cur_tel.PROCESSDATE||
                     ' PROCESSINFO='||cur_tel.PROCESSINFO);
        --循环依次取某一个号码的后8、7、6、5、4、3、2位，与号码模式表的每条数据相匹配
        v_flag:=0;
        v_low_fee_max:=-1;
        FOR I IN 1 .. 7 LOOP--取"最低消费"最高的模式
           v_telnum(I) := SUBSTR(cur_tel.INV_ID,I+3,9-I);
           FOR J IN 1 .. v_rec_count LOOP
           IF(v_pattern(J).telpattern=v_telnum(I)) THEN
              if(v_pattern(J).low_consum_fee>v_low_fee_max) then
                 v_low_fee_max:=v_pattern(J).low_consum_fee;
                 v_low_pre_max:=v_pattern(J).low_consum_pre;
                 v_low_time_max:=v_pattern(J).low_inservice_time;
                 v_privid_max:=v_pattern(J).privid;
                 v_patterntype:=v_pattern(J).patterntype;
         dbms_output.put_line('-->cur_tel循环：v_low_fee_max='||v_low_fee_max||' v_low_pre_max='||v_low_pre_max||' v_low_time_max='||v_low_time_max||' v_privid_max='||v_privid_max||' v_patterntype='||v_patterntype);
              end if;
              v_flag:=1;
        dbms_output.put_line('-->v_flag='||v_flag);
           END IF;
           END LOOP;
        END LOOP;
        if v_flag=1 then
           update im_inv_telnum set low_consum_fee=v_low_fee_max,
                                    low_consum_pre=v_low_pre_max,
                                    low_inservice_time=v_low_time_max,
                                    memo=v_patterntype
           Where REGION=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --删除 IM_INV_APPEND表中重复的数据
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';

           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID',v_privid_max,sysdate);
           --匹配到优惠规则的processed 置为1
           update IM_ESHOP_TELNUM_INLOG set processed=1,processdate=sysdate
            where region=cur_tel.region
                  and INTIME>trunc(sysdate-3)
              and inv_id=cur_tel.inv_id
              and STATUS='1'
              and PROCESSED = '0';
           goto continue;
        END IF;
        --4到7位是区号，且后4位中不带'4'
        if SUBSTR(cur_tel.INV_ID,4,4)='0'||to_char(cur_tel.region) and
                instr(SUBSTR(cur_tel.INV_ID,8,4),'4')=0 THEN
           update im_inv_telnum set low_consum_fee=5800,
                  low_consum_pre=30000,
                  low_inservice_time=null,
                  memo='Region'
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --删除 IM_INV_APPEND表中重复的数据
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';

           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID','gl.base.yzhm_dm_58.500',sysdate);
           --匹配到优惠规则的processed 置为1
           update IM_ESHOP_TELNUM_INLOG set processed=1,processdate=sysdate
            where region=cur_tel.region
              and INTIME>trunc(sysdate-3)
              and inv_id=cur_tel.inv_id
              and STATUS='1'
              and PROCESSED = '0';
            goto continue;
        end if;
        --未匹配到号码入网预交为50
        update im_inv_telnum set low_consum_fee=0,low_consum_pre=5000,low_inservice_time=NULL,memo = null
         Where region=cur_tel.region
           and INV_ID=cur_tel.inv_id;
  --清理老优质号码优惠
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';
        --未匹配到号码规则的置为2(所有剩余普通号码均有预存的情况下，这种情况不存在，暂时注销)
        update IM_ESHOP_TELNUM_INLOG set processed=2,processdate=sysdate
         where region=cur_tel.region
           and INTIME>trunc(sysdate-3)
           and inv_id=cur_tel.inv_id
           and STATUS='1'
           and PROCESSED = '0';
        <<continue>>
        null;
      END;
  END LOOP;
commit;
EXCEPTION
  WHEN OTHERS THEN
  rollback;
END P_LOC_SHOPJXHM;
/

CREATE OR REPLACE PROCEDURE "P_LOC_SHOPJXHM_CHECK" (v_region number)
AS
  v_rec_count number(8);
  row_num number(8):=1;
  v_errtype varchar2(32);
  v_rightvalue varchar2(32);
  v_privid varchar2(32);
  v_orgid varchar2(16);
  type telArry is table of varchar2(20) index by binary_integer;
  type typ_rec is record(telpattern varchar2(20),low_consum_fee number(10), low_consum_pre number(10)
                        ,low_inservice_time number(10),privid varchar2(32));
  type typ_tab is table of typ_rec index by binary_integer;
  v_telnum     telArry;
  v_pattern    typ_tab;
BEGIN
  SELECT COUNT(*) INTO v_rec_count FROM Loc_cs_shopjxhmrule WHERE STATUS=1;
  dbms_output.put_line('-->v_rec_count='||v_rec_count);
  --SELECT ORGID||'.ES' INTO v_orgid FROM region_list WHERE REGION=v_region;
  --加载号码模式表的数据到数组中
  FOR cur_pat IN (SELECT TELPATTERN,LOW_CONSUM_FEE,LOW_CONSUM_PRE,LOW_INSERVICE_TIME,PRIVID FROM LOC_CS_SHOPJXHMRULE WHERE STATUS=1)LOOP
      begin
          v_pattern(row_num).telpattern:=cur_pat.Telpattern;
          v_pattern(row_num).low_consum_fee:=cur_pat.low_consum_fee;
          v_pattern(row_num).low_consum_pre:=cur_pat.low_consum_pre;
          v_pattern(row_num).low_inservice_time:=cur_pat.low_inservice_time;
          v_pattern(row_num).privid:=cur_pat.privid;
          --DBMS_OUTPUT.put_line(v_pattern(row_num).telpattern);
          row_num:=row_num+1;
      dbms_output.put_line('-->cur_pat循环：telpattern='||cur_pat.Telpattern||
                 ' low_consum_fee='||cur_pat.low_consum_fee||
                 ' low_consum_pre='||cur_pat.low_consum_pre||
                 ' low_inservice_time='||cur_pat.low_inservice_time||
                 ' privid='||cur_pat.privid);
    dbms_output.put_line('-->row_num='||row_num);
      end;
  END LOOP;
  FOR cur_tel IN (SELECT INV_ID,LOW_CONSUM_PRE,LOW_CONSUM_FEE,LOW_INSERVICE_TIME
                    FROM IM_INV_TELNUM
                    WHERE REGION=v_region
                    AND ORG_ID like 'SD.L_.ES') LOOP
      begin
      --匹配188普通号源
      dbms_output.put_line('-->cur_pat循环：INV_ID='||cur_tel.INV_ID||
                      ' LOW_CONSUM_PRE='||cur_tel.LOW_CONSUM_PRE||
                      ' LOW_CONSUM_FEE='||cur_tel.LOW_CONSUM_FEE||
                      ' LOW_INSERVICE_TIME='||cur_tel.LOW_INSERVICE_TIME);
        IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
           IF cur_tel.low_consum_fee<>50 or cur_tel.low_consum_fee is null THEN
              v_errtype:='lowfee';
              v_rightvalue:='50';
        dbms_output.put_line('-->v_errtype='||v_errtype ||' v_rightvalue='||v_rightvalue);
              INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status)
                   VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0);
           END IF;
           IF cur_tel.Low_Consum_Pre<>300 or cur_tel.Low_Consum_Pre is null THEN
              v_errtype:='lowprefee';
              v_rightvalue:='300';
        dbms_output.put_line('-->v_errtype='||v_errtype ||' v_rightvalue='||v_rightvalue);
              INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status)
                   VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0);
           END IF;
           IF cur_tel.Low_Inservice_Time<>999 or cur_tel.Low_Inservice_Time is null THEN
              v_errtype:='lowintime';
              v_rightvalue:='5';
        dbms_output.put_line('-->v_errtype='||v_errtype ||' v_rightvalue='||v_rightvalue);
              INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status)
                   VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0);
           END IF;
           BEGIN
               SELECT ATTR_VALUE INTO v_privid
               FROM IM_INV_APPEND
               WHERE REGION=v_region
               AND INV_ID=cur_tel.INV_ID
               AND ATTR_ID='PRIV_ID';
         dbms_output.put_line('-->v_privid='||v_privid);
               IF v_privid IS NOT NULL THEN
                  v_errtype:='privid';
                  v_rightvalue:='';
          dbms_output.put_line('-->v_errtype='||v_errtype||' v_rightvalue='||v_rightvalue);
                  INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status,note)
                       VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0,'188普通');
               END IF;
           EXCEPTION
               WHEN OTHERS THEN
               NULL;
           END;
           goto continue;
        END IF;
        --循环依次取某一个号码的后8、7、6、5、4、3、2位，与号码模式表的每条数据相匹配
        FOR I IN 1 .. 7 LOOP
           v_telnum(I) := SUBSTR(cur_tel.INV_ID,I+3,9-I);
       dbms_output.put_line('-->v_telnum(I)='||v_telnum(I));
           FOR J IN 1 .. v_rec_count LOOP
               IF(v_pattern(J).telpattern=v_telnum(I)) THEN
                   --lowfee-最低消费 lowprefee-最低预存 lowintime-最低在网时长 privid-优惠编码
                   IF(v_pattern(J).low_consum_fee<>cur_tel.low_consum_fee OR
                     (v_pattern(J).low_consum_fee>=0 AND cur_tel.low_consum_fee IS NULL)) THEN
                      v_errtype:='lowfee';
                      v_rightvalue:=v_pattern(J).low_consum_fee;
                      INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status,note)
                           VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0,v_pattern(J).telpattern);
                   END IF;
                   IF(v_pattern(J).low_consum_pre<>cur_tel.low_consum_pre OR
                      (v_pattern(J).low_consum_pre>=0 AND cur_tel.low_consum_pre IS NULL)) THEN
                      v_errtype:='lowprefee';
                      v_rightvalue:=v_pattern(J).low_consum_pre;
                      INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status,note)
                           VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0,v_pattern(J).telpattern);
                   END IF;
                   IF(v_pattern(J).low_inservice_time<>cur_tel.low_inservice_time OR
                     (v_pattern(J).low_inservice_time>=0 AND cur_tel.low_inservice_time IS NULL)) THEN
                      v_errtype:='lowintime';
                      v_rightvalue:=v_pattern(J).low_inservice_time;
                      INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status,note)
                           VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0,v_pattern(J).telpattern);
                   END IF;
                   BEGIN
                       SELECT ATTR_VALUE INTO v_privid
                         FROM IM_INV_APPEND
                        WHERE REGION=v_region
                        AND INV_ID=cur_tel.INV_ID
                        AND ATTR_ID='PRIV_ID';
            dbms_output.put_line('-->v_privid='||v_privid);
                       IF(v_pattern(J).privid<>v_privid or
                         (v_pattern(J).privid is not null and v_privid is null) or
                         (v_pattern(J).privid is null and v_privid is not null)) THEN
                          v_errtype:='privid';
                          v_rightvalue:=v_pattern(J).privid;
                          INSERT INTO IM_ESHOP_TELNUM_ERR(region,inv_id,checkdate,errtype,rightValue,status,note)
                               VALUES(v_region,cur_tel.INV_ID,SYSDATE,v_errtype,v_rightvalue,0,v_pattern(J).telpattern);
                       END IF;
                   EXCEPTION
                       WHEN OTHERS THEN
                       NULL;
                   END;
                   goto continue;
               END IF;
           END LOOP;
        END LOOP;
        <<continue>>
        null;
      end;
  END LOOP;
commit;
EXCEPTION
  WHEN OTHERS THEN
  rollback;
END P_LOC_SHOPJXHM_CHECK;
/

CREATE OR REPLACE PROCEDURE "P_LOC_SHOPJXHM_TMP" (i_region number,i_tag number)
AS
  v_rec_count number(8);
  row_num number(8):=1;
  v_orgid varchar2(16);
  v_region number(5);
  v_flag number(1);
  v_low_fee_max number(10);
  v_low_pre_max number(10);
  v_low_time_max number(10);
  v_privid_max varchar2(32);
  v_patterntype varchar2(32);
  type telArry is table of varchar2(20) index by binary_integer;
  type typ_rec is record(telpattern varchar2(20),low_consum_fee number(10), low_consum_pre number(10)
                        ,low_inservice_time number(10),privid varchar2(32),patterntype varchar2(32));
  type typ_tab is table of typ_rec index by binary_integer;
  v_telnum     telArry;
  v_pattern    typ_tab;
BEGIN
  IF i_tag=1 THEN
     v_region:=i_region;
  ELSE
     v_region:=999;
  END IF;
  SELECT COUNT(*) INTO v_rec_count FROM Loc_cs_shopjxhmrule WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1;
  dbms_output.put_line('-->v_rec_count='||v_rec_count);
  --加载号码模式表的数据到数组中
  FOR cur_pat IN (SELECT TELPATTERN,LOW_CONSUM_FEE,LOW_CONSUM_PRE,LOW_INSERVICE_TIME,PRIVID,PATTERNTYPE FROM LOC_CS_SHOPJXHMRULE WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1)LOOP
      BEGIN
          v_pattern(row_num).telpattern:=cur_pat.Telpattern;
          v_pattern(row_num).low_consum_fee:=cur_pat.low_consum_fee;
          v_pattern(row_num).low_consum_pre:=cur_pat.low_consum_pre;
          v_pattern(row_num).low_inservice_time:=cur_pat.low_inservice_time;
          v_pattern(row_num).privid:=cur_pat.privid;
          v_pattern(row_num).patterntype:=cur_pat.patterntype;
          --DBMS_OUTPUT.put_line(v_pattern(row_num).telpattern);
          row_num:=row_num+1;
      dbms_output.put_line('-->cur_pat循环：telpattern='||cur_pat.Telpattern||
                 ' low_consum_fee='||cur_pat.low_consum_fee||
                 ' low_consum_pre='||cur_pat.low_consum_pre||
                 ' low_inservice_time='||cur_pat.low_inservice_time||
                 ' privid='||cur_pat.privid);
      dbms_output.put_line('-->row_num='||row_num);
      END;
  END LOOP;
  SELECT ORGID||'.ES' INTO v_orgid FROM region_list WHERE REGION=i_region;
   dbms_output.put_line('-->v_orgid='||v_orgid);
  FOR cur_tel IN (SELECT REGION,INV_ID,PROCESSED,PROCESSDATE,PROCESSINFO
                    FROM IM_ESHOP_TELNUM_INLOG_TMP
                   WHERE REGION=i_region
                     AND INTIME>=trunc(sysdate-10)  --时间
                     AND STATUS ='1'
                     AND PROCESSED = '0' for update) LOOP
    dbms_output.put_line('-->cur_tel循环：REGION='||cur_tel.REGION||
                      ' INV_ID='||cur_tel.INV_ID||
                      ' PROCESSED='||cur_tel.PROCESSED||
                      ' PROCESSDATE='||cur_tel.PROCESSDATE||
                      ' PROCESSINFO='||cur_tel.PROCESSINFO);
      BEGIN
        --循环依次取某一个号码的后8、7、6、5、4、3、2位，与号码模式表的每条数据相匹配
        v_flag:=0;
        v_low_fee_max:=-1;
    DBMS_OUTPUT.put_line('-->v_flag=' || v_flag);
        FOR I IN 1 .. 7 LOOP--取"最低消费"最高的模式
           v_telnum(I) := SUBSTR(cur_tel.INV_ID,I+3,9-I);
       DBMS_OUTPUT.put_line('-->v_telnum(I)=' || v_telnum(I));
           FOR J IN 1 .. v_rec_count LOOP
           IF(v_pattern(J).telpattern=v_telnum(I)) THEN
      DBMS_OUTPUT.put_line('-->v_pattern(J).telpattern=' || v_pattern(J).telpattern);
              if(v_pattern(J).low_consum_fee>v_low_fee_max) then
        DBMS_OUTPUT.put_line('-->v_pattern(J).low_consum_fee=' || v_pattern(J).low_consum_fee);
        DBMS_OUTPUT.put_line('-->v_low_fee_max=' || v_low_fee_max);
                 v_low_fee_max:=v_pattern(J).low_consum_fee;
                 v_low_pre_max:=v_pattern(J).low_consum_pre;
                 v_low_time_max:=v_pattern(J).low_inservice_time;
                 v_privid_max:=v_pattern(J).privid;
                 v_patterntype:=v_pattern(J).patterntype;
         dbms_output.put_line('-->v_low_fee_max='||v_low_fee_max||
                    ' v_low_pre_max='||v_low_fee_max||
                    ' v_low_time_max='||v_low_time_max||
                    ' v_privid_max='||v_privid_max||
                    ' v_patterntype='||v_patterntype);
              end if;
              v_flag:=1;
        DBMS_OUTPUT.put_line('-->v_flag=' || v_flag);
           END IF;
           END LOOP;
        END LOOP;
        if v_flag=1 then
           --当号码匹配到一条规则且此号码是属于188号码时，当此规则的"最低消费"<188的"最低消费"时,取188的规则
           IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
      DBMS_OUTPUT.put_line('-->当号码匹配到一条规则且此号码是属于188号码');
              IF v_low_fee_max<5000 THEN
                 v_low_fee_max:=5000;
                 v_low_pre_max:=30000;
                 v_low_time_max:=5;
                 v_patterntype:='188';
                 v_privid_max:='gl.base.999649_24.500';
         dbms_output.put_line('-->当v_flag=时更新数据v_low_fee_max='||v_low_fee_max||
                               ' v_low_pre_max='||v_low_pre_max||
                               ' v_low_time_max='||v_low_time_max||
                               ' v_patterntype='||v_patterntype||
                               ' v_privid_max='||v_privid_max);
              END IF;
           END IF;
           update tmp_im_inv_telnum set low_consum_fee=v_low_fee_max,
                                    low_consum_pre=v_low_pre_max,
                                    low_inservice_time=v_low_time_max,
                                    memo=v_patterntype
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --匹配到优惠规则的processed 置为1
           update IM_ESHOP_TELNUM_INLOG_TMP set processed=1,processdate=sysdate
            where region=cur_tel.region
              and INTIME>trunc(sysdate-10)
              and inv_id=cur_tel.inv_id
              and STATUS='1'
              and PROCESSED = '0';
           goto abc;
        end if;
        --
    DBMS_OUTPUT.put_line('匹配188普通号源');
        IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
           update tmp_im_inv_telnum set low_consum_fee=5000,
                                    low_consum_pre=30000,
                                    low_inservice_time=5,
                                    memo='188'
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --匹配到优惠规则的processed 置为1
           update IM_ESHOP_TELNUM_INLOG_TMP set processed=1,processdate=sysdate
            where region=cur_tel.region
              and INTIME>trunc(sysdate-10)
              and inv_id=cur_tel.inv_id
              and STATUS='1'
              and PROCESSED = '0';
           goto abc;
        END IF;
        --未匹配到号码入网预交为50
    DBMS_OUTPUT.put_line('-->未匹配到号码入网预交为50');
        update tmp_im_inv_telnum set low_consum_fee=0,low_consum_pre=5000,low_inservice_time=NULL
         Where region=cur_tel.region
           and INV_ID=cur_tel.inv_id;
        --未匹配到号码规则的置为2(所有剩余普通号码均有预存的情况下，这种情况不存在，暂时注销)
    DBMS_OUTPUT.put_line('-->未匹配到号码规则的置为2');
        update IM_ESHOP_TELNUM_INLOG_TMP set processed=2,processdate=sysdate
         where region=cur_tel.region
           and INTIME>trunc(sysdate-10)
           and inv_id=cur_tel.inv_id
           and STATUS='1'
           and PROCESSED = '0';
        <<abc>>
        null;
      END;
  END LOOP;
commit;
EXCEPTION
  WHEN OTHERS THEN
  rollback;
END P_LOC_SHOPJXHM_TMP;
/

CREATE OR REPLACE PROCEDURE "P_LOC_ALLESHOPTELNUM" (i_region number,i_tag number)
AS
  v_rec_count number(8);
  row_num number(8):=1;
  v_orgid varchar2(16);
  v_region number(5);
  v_flag number(1);
  v_low_fee_max number(10);
  v_low_pre_max number(10);
  v_low_time_max number(10);
  v_privid_max varchar2(32);
  v_patterntype varchar2(32);
  type telArry is table of varchar2(20) index by binary_integer;
  type typ_rec is record(telpattern varchar2(20),low_consum_fee number(10), low_consum_pre number(10)
                        ,low_inservice_time number(10),privid varchar2(32),patterntype varchar2(32));
  type typ_tab is table of typ_rec index by binary_integer;
  v_telnum     telArry;
  v_pattern    typ_tab;
BEGIN
  IF i_tag=1 THEN
     v_region:=i_region;
  ELSE
     v_region:=999;
  END IF;
  SELECT COUNT(*) INTO v_rec_count FROM Loc_cs_shopjxhmrule WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1;
  dbms_output.put_line('-->v_rec_count='||v_rec_count);
  --加载号码模式表的数据到数组中
  FOR cur_pat IN (SELECT TELPATTERN,LOW_CONSUM_FEE,LOW_CONSUM_PRE,LOW_INSERVICE_TIME,PRIVID,PATTERNTYPE FROM LOC_CS_SHOPJXHMRULE WHERE REGION=v_region AND STATUS=1 AND CHECKFLAG=1)LOOP
      BEGIN
          v_pattern(row_num).telpattern:=cur_pat.Telpattern;
          v_pattern(row_num).low_consum_fee:=cur_pat.low_consum_fee;
          v_pattern(row_num).low_consum_pre:=cur_pat.low_consum_pre;
          v_pattern(row_num).low_inservice_time:=cur_pat.low_inservice_time;
          v_pattern(row_num).privid:=cur_pat.privid;
          v_pattern(row_num).patterntype:=cur_pat.patterntype;
          --DBMS_OUTPUT.put_line(v_pattern(row_num).telpattern);
          row_num:=row_num+1;
      dbms_output.put_line('-->cur_pat循环：TELPATTERN='||cur_pat.TELPATTERN||
                        ' LOW_CONSUM_FEE='||cur_pat.LOW_CONSUM_FEE||
                        ' LOW_CONSUM_PRE='||cur_pat.LOW_CONSUM_PRE||
                        ' LOW_INSERVICE_TIME='||cur_pat.LOW_INSERVICE_TIME||
                        ' PRIVID='||cur_pat.PRIVID||
                        ' PATTERNTYPE='||cur_pat.PATTERNTYPE);
     dbms_output.put_line('-->row_num='||row_num);
      END;
  END LOOP;
  --SELECT ORGID||'.ES' INTO v_orgid FROM region_list WHERE REGION=i_region;
  FOR cur_tel IN (SELECT INV_ID,LOW_CONSUM_PRE,LOW_CONSUM_FEE,LOW_INSERVICE_TIME,REGION
                    FROM IM_INV_TELNUM
                   WHERE REGION=i_region
                     AND ORG_ID like 'SD.L_.ES' for update) LOOP
           DBMS_OUTPUT.put_line('-->cur_tel循环：INV_ID=' ||cur_tel.INV_ID||
                                       ' LOW_CONSUM_PRE='||cur_tel.LOW_CONSUM_PRE||
                                       ' LOW_CONSUM_FEE='||cur_tel.LOW_CONSUM_FEE||
                                       ' LOW_INSERVICE_TIME='||cur_tel.LOW_INSERVICE_TIME||
                                       ' REGION='||cur_tel.REGION);
      BEGIN
        --循环依次取某一个号码的后8、7、6、5、4、3、2位，与号码模式表的每条数据相匹配
        v_flag:=0;
        v_low_fee_max:=-1;
    DBMS_OUTPUT.put_line('-->v_flag=' || v_flag);
        FOR I IN 1 .. 7 LOOP--取"最低消费"最高的模式
           v_telnum(I) := SUBSTR(cur_tel.INV_ID,I+3,9-I);
       DBMS_OUTPUT.put_line('-->v_telnum(I)=' || v_telnum(I));
           FOR J IN 1 .. v_rec_count LOOP
           IF(v_pattern(J).telpattern=v_telnum(I)) THEN
       DBMS_OUTPUT.put_line('-->v_pattern(J).telpattern=' || v_pattern(J).telpattern);
              if(v_pattern(J).low_consum_fee>v_low_fee_max) then
         DBMS_OUTPUT.put_line('-->v_pattern(J).low_consum_fee=' || v_pattern(J).low_consum_fee);
        DBMS_OUTPUT.put_line('-->v_low_fee_max=' || v_low_fee_max);
                 v_low_fee_max:=v_pattern(J).low_consum_fee;
                 v_low_pre_max:=v_pattern(J).low_consum_pre;
                 v_low_time_max:=v_pattern(J).low_inservice_time;
                 v_privid_max:=v_pattern(J).privid;
                 v_patterntype:=v_pattern(J).patterntype;
              end if;
              v_flag:=1;
        DBMS_OUTPUT.put_line('-->v_flag=' || v_flag);
           END IF;
           END LOOP;
        END LOOP;
        if v_flag=1 then
           --当号码匹配到一条规则且此号码是属于188号码时，当此规则的"最低消费"<188的"最低消费"时,取188的规则
           IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
      DBMS_OUTPUT.put_line('-->SUBSTR(cur_tel.INV_ID,1,3)=' || SUBSTR(cur_tel.INV_ID,1,3));
              IF v_low_fee_max<5000 THEN
                 v_low_fee_max:=5000;
                 v_low_pre_max:=30000;
                 v_low_time_max:=5;
                 v_patterntype:='188';
                 v_privid_max:='gl.base.999649_24.500';
         dbms_output.put_line('-->当INV_ID=188时，v_low_fee_max='||v_low_fee_max||
                            ' v_low_pre_max='||v_low_pre_max||
                            ' v_low_time_max='||v_low_time_max||
                            ' v_patterntype='||v_patterntype||
                            ' v_privid_max='||v_privid_max);
              END IF;
           END IF;
           update im_inv_telnum set low_consum_fee=v_low_fee_max,
                                    low_consum_pre=v_low_pre_max,
                                    low_inservice_time=v_low_time_max,
                                    memo=v_patterntype
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --删除 IM_INV_APPEND表中重复的数据
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';
           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID',v_privid_max,sysdate);
           goto continue;
        end if;
        --匹配188普通号源
        IF SUBSTR(cur_tel.INV_ID,1,3)='188' THEN
      DBMS_OUTPUT.put_line('SUBSTR(cur_tel.INV_ID,1,3)=' || SUBSTR(cur_tel.INV_ID,1,3));
           update im_inv_telnum set low_consum_fee=5000,
                                    low_consum_pre=30000,
                                    low_inservice_time=5,
                                    memo='188'
           Where region=cur_tel.region
             and INV_ID=cur_tel.inv_id;
           --删除 IM_INV_APPEND表中重复的数据
       DBMS_OUTPUT.put_line('删除 IM_INV_APPEND表中重复的数据');
           delete from IM_INV_APPEND
                 where region=cur_tel.region
                   and inv_id=cur_tel.inv_id
                   and attr_id='PRIV_ID';
      DBMS_OUTPUT.put_line('插入 IM_INV_APPEND表中重复的数据');
           insert into IM_INV_APPEND(REGION,INV_ID,RES_KIND_ID,RES_TYPE_ID,ATTR_ID,ATTR_VALUE,CREATE_DATE)
                values (cur_tel.region,cur_tel.inv_id,' ','rsclTgsm','PRIV_ID','gl.base.999649_24.500',sysdate);
           goto continue;
        END IF;
        --未匹配到号码入网预交为50
    DBMS_OUTPUT.put_line('未匹配到号码入网预交为50');
        update im_inv_telnum set low_consum_fee=0,low_consum_pre=5000,low_inservice_time=NULL
         Where region=cur_tel.region
           and INV_ID=cur_tel.inv_id;
        <<continue>>
        null;
      END;
  END LOOP;
commit;
EXCEPTION
  WHEN OTHERS THEN
  rollback;
END p_loc_alleshoptelnum;
/

CREATE OR REPLACE PROCEDURE "LOC_RANKLIST_APPLY" is
begin
  --删除排行表中当前数据
  delete from psi_info_mob_rankinglist t where t.ranking_type = 'pickRank';
  ---统计最近30日提货数据
  for v_record in (select tt.amt,
                          tt.res_type_id,
                          (select n.brand_id
                             from im_res_type n
                            where n.res_type_id = tt.res_type_id
                              and rownum = 1) brand_id
                     from (select t.amt, t.res_type_id
                             from (select sum(b.apply_num) amt, b.res_type_id
                                     from psi_flow_apply_batch b
                                    where b.create_date > sysdate - 30
                                      and b.batch_id >
                                          to_char(sysdate - 30, 'yyyymmdd')
                                    group by b.res_type_id) t
                            order by t.amt desc) tt
                    where rownum <= 30) loop
    dbms_output.put_line('-->v_record循环：amt='||v_record.amt||
                       ' res_type_id='||v_record.res_type_id||
                       ' brand_id='||v_record.brand_id);
    insert into psi_info_mob_rankinglist
      (Ranking_type,
       Res_type_id,
       Brand_id,
       Static_amt,
       Static_amt1,
       Static_amt2,
       Static_amt3,
       Org_id)
    values
      ('pickRank',
       v_record.res_type_id,
       v_record.brand_id,
       v_record.amt,
       null,
       null,
       null,
       null);
     dbms_output.put_line('-->表 psi_info_mob_rankinglist 中相应的数据更新成功');
    commit;
  end loop;
  commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end loc_ranklist_apply;
/

CREATE OR REPLACE PROCEDURE "PRC_LOC_PSI_STAT_MONTHORDERRPT" (v_region in number) is
  v_proc_result varchar2(32);
  v_cnt         number(8);
begin
  delete from psi_loc_stat_monthorderrpt_mid t where t.region = v_region;
  --审阅状态
  begin
    SELECT t.param_value
      into v_proc_result
      FROM psi_param_value t
     WHERE t.param_id = 'TARGET_STAT_SALEINFO_FLOWSTATUS';
  exception
    when others then
      v_proc_result := '';
  end;
  --如果这个参数没有配置的话 ，默认是UNAPPROVEBYPROVADM（待省采购部审批）
  if v_proc_result = '' or v_proc_result is null then
    v_proc_result := 'UNAPPROVEBYPROVADM';
  end if;
  dbms_output.put_line('-->v_proc_result='||v_proc_result);
  for v_record in (select c.apply_num,
                          c.res_type_id,
                          b.org_id,
                          substr(b.org_id, 0, 8) areaid,
                          to_char(sysdate, 'yyyymm') month
                     from psi_flow_proc_log    a,
                          psi_flow_apply       b,
                          psi_flow_apply_batch c,
                          psi_dict_item        d
                    where a.region = v_region
                      and a.region = b.region
                      and trunc(a.create_date, 'dd') =
                          trunc(TO_DATE('20201017','yyyy-mm-dd') - 1, 'dd')
                      and a.proc_result = v_proc_result
                      and a.flow_id = b.flow_id
                      and b.region = c.region
                      and c.flow_id = b.flow_id
                      and b.apply_type = d.dict_id
                      and d.group_id = 'TARGET_STAT_FLOWAPPLY') loop
    dbms_output.put_line('-->v_record循环：apply_num='||v_record.apply_num||
                       ' res_type_id='||v_record.res_type_id||
                       ' org_id='||v_record.org_id||
                       ' areaid='||v_record.areaid||
                       ' month='||v_record.month);
    --插入中间表
    insert into psi_loc_stat_monthorderrpt_mid
      (REGION, AREAID, ORG_ID, RES_TYPE_ID, STAT_MONTH, ORDER_NUM)
    values
      (v_region,
       v_record.areaid,
       v_record.org_id,
       v_record.res_type_id,
       v_record.month,
       v_record.apply_num);
  end loop;
  commit;
  --统计县订单量
  insert into psi_loc_stat_monthorderrpt_mid
    (REGION, areaid, ORG_ID, RES_TYPE_ID, STAT_MONTH, ORDER_NUM)
    select t.region,
           t.areaid,
           t.areaid,
           t.res_type_id,
           t.stat_month,
           sum(t.order_num)
      from psi_loc_stat_monthorderrpt_mid t
     where t.region = v_region
     group by t.region, t.areaid, t.res_type_id, t.stat_month;
  commit;
  --统计市订单量
  insert into psi_loc_stat_monthorderrpt_mid
    (REGION, areaid, ORG_ID, RES_TYPE_ID, STAT_MONTH, ORDER_NUM)
    select t.region,
           substr(t.areaid,1,5),
           substr(t.areaid,1,5),
           t.res_type_id,
           t.stat_month,
           sum(t.order_num)
      from psi_loc_stat_monthorderrpt_mid t
     where t.region = v_region
       and t.areaid <> t.org_id
     group by t.region, substr(t.areaid,1,5),t.res_type_id, t.stat_month;
  commit;
  --将中将表数据统计到最终表
  for v_record1 in (select * from psi_loc_stat_monthorderrpt_mid) loop
    dbms_output.put_line('-->v_record1循环：REGION='||v_record1.REGION||
                      ' AREAID='||v_record1.AREAID||
                      ' ORG_ID='||v_record1.ORG_ID||
                      ' RES_TYPE_ID='||v_record1.RES_TYPE_ID||
                      ' STAT_MONTH='||v_record1.STAT_MONTH||
                      ' ORDER_NUM='||v_record1.ORDER_NUM);
    select count(*)
      into v_cnt
      from psi_loc_stat_monthorderrpt t
     where t.region = v_region
       and t.stat_month = v_record1.stat_month
       and t.org_id = v_record1.org_id
       and t.res_type_id = v_record1.res_type_id;
     dbms_output.put_line('-->v_record1循环：v_cnt='||v_cnt);
    if v_cnt = 0 then
      --如果没有记录插入
      insert into psi_loc_stat_monthorderrpt
        (REGION, ORG_ID, RES_TYPE_ID, STAT_MONTH, ORDER_NUM, Update_date)
      values
        (v_record1.region,
         v_record1.org_id,
         v_record1.res_type_id,
         v_record1.stat_month,
         v_record1.order_num,
         sysdate);
    else
      --有则更新
      update psi_loc_stat_monthorderrpt t
         set t.order_num   = t.order_num + v_record1.order_num,
             t.update_date = sysdate
       where t.region = v_record1.region
         and t.org_id = v_record1.org_id
         and t.stat_month = v_record1.stat_month
         and t.res_type_id = v_record1.res_type_id;
    end if;
  end loop;
commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
             ' || SQLERRM);
end prc_loc_psi_stat_monthorderrpt;
/

CREATE OR REPLACE PROCEDURE "PRC_LOC_STAT_ZDQKCHIS" (v_region in number) is
begin
  --先删除所有信息
  delete loc_stat_zdqkchis WHERE region=v_region;
  --每15分钟入库一次库存量信息
  dbms_output.put_line('-->开始插入数据');
  insert into loc_stat_zdqkchis
    (REGION,
     AREAID,
     ORGID,
     STOCKDATE,
     STOCKNUM,
     SETTLE_MODE,
     BRAND,
     RES_TYPE_ID)
    select a.region,
           substr(a.org_id, 0, 8),
           a.org_id,
           (case
             when a.ldstore_date > to_date('20140101', 'yyyymmdd') then
              a.ldstore_date
             else
              to_date('20131231', 'yyyymmdd')
           end) stockdate,
           count(a.inv_id),
           a.settle_mode,
           b.brand_id,
           b.res_type_id
      from im_inv_mobtel a,im_res_type b
     where a.inv_status = 'INSTORE'
       and a.busi_status = 'USABLE'
       and a.region = v_region
       and a.res_type_id=b.res_type_id
     group by a.region,
              substr(a.org_id, 0, 8),
              a.org_id,
              a.ldstore_date,
              a.settle_mode,
              b.brand_id,
              b.res_type_id;
  dbms_output.put_line('-->插入数据成功');
  commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end prc_loc_stat_zdqkchis;
/

CREATE OR REPLACE PROCEDURE "AP_PSI_STAT_DAYPSI_SD"
 IS
  p_region      number(5);
  p_orgId       varchar2(32);
  p_resKindId   varchar2(32);
  p_resTypeId   varchar2(32);
  p_brandId     varchar2(32);
  p_specialAttr varchar2(32);
  p_ownType     varchar2(32);
  p_supplierId  varchar2(32);
  p_statDay     varchar2(32) := to_char(TO_DATE('20201017','yyyy-mm-dd') - 1, 'YYYYMMDD');
  p_statType    varchar2(32) := 'COMMON';
  p_groupId     varchar2(32);
  p_telSegment  varchar2(32);
  v_stockNum    number(10);
  v_sum         number(10);
begin
  --首先删除昨天的数据
  delete from psi_stat_daypsi t
   where t.stat_day = p_statDay
     and t.stat_type = 'COMMON';
  commit;
  ---------初始化当天日报
  begin
    insert into psi_stat_daypsi
      (REGION,
       ORG_ID,
       RES_KIND_ID,
       STAT_DAY,
       STAT_TYPE,
       BRAND_ID,
       RES_TYPE_ID,
       SPECIAL_ATTR,
       OWNER_TYPE,
       SUPPLIER_ID,
       ORG_TYPE,
       ORIGINAL_NUM,
       LDSTORE_NUM,
       REFUND_NUM,
       ASIGIN_NUM,
       ASIGOUT_NUM,
       SALE_NUM,
       SALEBACK_NUM,
       ADJUSTIN_NUM,
       ADJUSTOUT_NUM,
       EXCHGIN_NUM,
       EXCHGOUT_NUM,
       STOCK_NUM,
       CREATE_DATE,
       GROUP_ID,
       TEL_SEGMENT
       )
      (select REGION,
              ORG_ID,
              RES_KIND_ID,
              p_statDay,
              STAT_TYPE,
              BRAND_ID,
              RES_TYPE_ID,
              SPECIAL_ATTR,
              OWNER_TYPE,
              SUPPLIER_ID,
              OWNER_TYPE,
              STOCK_NUM,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              sysdate,
              GROUP_ID,
              TEL_SEGMENT
         from PSI_STAT_DAYPSI a
        where a.stat_day =
              to_char((to_date(p_statDay, 'YYYYMMDD') - 1), 'YYYYMMDD'));
    commit;
  end;
  --循环处理各个单位的数据
  for invList in (select t.region,
                         t.res_kind_id,
                         t.org_id,
                         t.res_type_id,
                         t.supplier_id,
                         t.owner_type,
                         t.group_id,
                         t.tel_segment,
                         nvl(sum(t.stock_num), 0) countNum
                    from PSI_STK_REALTIME_MSTOCK t
                   where to_char(t.change_date,'YYYYMMDD') = p_statDay
                     and (t.inv_status = 'INSTORE' or t.inv_status = 'ONWAY')
                     group by t.region,
                              t.res_kind_id,
                              t.org_id,
                              t.res_type_id,
                              t.supplier_id,
                              t.owner_type,
                              t.group_id,
                              t.tel_segment) loop

    begin
      p_orgId      := invList.org_id;
      p_resTypeId  := invList.res_type_id;
      p_ownType    := invList.owner_Type;
      p_supplierId := invList.supplier_Id;
      p_region     := invList.region;
      p_resKindId  := invList.res_kind_id;
      p_groupId    := invList.group_id;
      p_telSegment := invList.tel_segment;
      v_stockNum   := invList.countNum;
       DBMS_OUTPUT.put_line('-->invList循环：p_orgId='||p_orgId ||
                       ' p_resTypeId='||p_resTypeId||
                       ' p_ownType='||p_ownType||
                       ' p_supplierId='||p_supplierId||
                       ' p_region='||p_region||
                       ' p_resKindId='||p_resKindId||
                       ' p_groupId='||p_groupId||
                       ' p_telSegment='||p_telSegment||
                       ' v_stockNum='||v_stockNum);

      select t.brand_id
        into p_brandId
        from im_res_type t
       where t.res_type_id = p_resTypeId;
      select count(1)
        into v_sum
        from psi_stat_daypsi t
       where t.region = p_region
         and t.org_id = p_orgId
         and t.res_kind_id = p_resKindId
         and t.stat_day = p_statDay
         and t.res_type_id = p_resTypeId
         and t.owner_type = p_ownType
         and t.supplier_id = p_supplierId
         and (t.group_id = p_groupId or
              (t.group_id is null and p_groupId is null)
              )
         and (t.tel_segment = p_telSegment or
              (t.tel_segment is null and p_telSegment is null)
              );
    DBMS_OUTPUT.put_line('-->invList循环：p_brandId='||p_brandId);
    DBMS_OUTPUT.put_line('-->invList循环：v_sum='||v_sum);

      if v_sum > 0 then
        update psi_stat_daypsi t
           set t.stock_num = v_stockNum
         where t.region = p_region
           and t.org_id = p_orgId
           and t.res_kind_id = p_resKindId
           and t.stat_day = p_statDay
           and t.res_type_id = p_resTypeId
           and t.owner_type = p_ownType
           and t.supplier_id = p_supplierId
           and (t.group_id = p_groupId or
                (t.group_id is null and p_groupId is null)
                )
           and (t.tel_segment = p_telSegment or
                (t.tel_segment is null and p_telSegment is null)
                );
        commit;
      else
        --数据插入
        INSERT INTO psi_stat_daypsi
          (REGION,
           ORG_ID,
           RES_KIND_ID,
           STAT_DAY,
           STAT_TYPE,
           BRAND_ID,
           RES_TYPE_ID,
           SPECIAL_ATTR,
           OWNER_TYPE,
           SUPPLIER_ID,
           ORG_TYPE,
           ORIGINAL_NUM,
           LDSTORE_NUM,
           REFUND_NUM,
           ASIGIN_NUM,
           ASIGOUT_NUM,
           SALE_NUM,
           SALEBACK_NUM,
           ADJUSTIN_NUM,
           ADJUSTOUT_NUM,
           EXCHGIN_NUM,
           EXCHGOUT_NUM,
           STOCK_NUM,
           CREATE_DATE,
           GROUP_ID,
           TEL_SEGMENT
           )
        VALUES
          (p_region,
           p_orgId,
           p_resKindId,
           p_statDay,
           p_statType,
           p_brandId,
           p_resTypeId,
           p_specialAttr,
           p_ownType,
           p_supplierId,
           p_ownType,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           v_stockNum,
           sysdate,
           p_groupId,
           p_telSegment
           );
        commit;
      end if;
      commit;
    end;
  end loop;
  for changeList in (select org_id,
                            res_type_id,
                            owner_type,
                            supplier_id,
                            busi_type,
                            sub_busi_type,
                            inout_flag,
                             group_id,
                             tel_segment,
                            sum(amount) Amount
                       from psi_busi_stock_log_his t
                      where t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                        and t.create_date <
                            to_date(p_statDay, 'YYYYMMDD') + 1
                        and ((t.busi_type = 'LDSTORE' and t.inout_flag = 'IN')
                        or (t.busi_type = 'ROLLBACK' and t.inout_flag = 'OUT')
                        or (t.busi_type = 'ASIGOUT' and t.inout_flag = 'IN')
                        or (t.busi_type = 'ASIGOUT' and t.inout_flag = 'OUT')
                        or (t.busi_type = 'SALE' and t.inout_flag = 'OUT')
                        or (t.busi_type = 'SALEBACK' and t.inout_flag = 'IN')
                        or (t.busi_type in ('ASIGIN', 'ASIGOUT') and t.inout_flag = 'IN')
                        or (t.busi_type in ('ASIGIN', 'ASIGOUT') and t.inout_flag = 'OUT')
                        or (t.busi_type = 'EXCHGIN' and t.inout_flag = 'IN')
                        or (t.busi_type = 'EXCHGOUT' and t.inout_flag = 'OUT'))
                      group by t.org_id,
                               t.res_type_id,
                               t.owner_type,
                               t.supplier_id,
                               t.busi_type,
                               t.sub_busi_type,
                               t.inout_flag,
                               t.group_id,
                               t.tel_segment) loop
    begin
    dbms_output.put_line('-->changeList循环：org_id='||changeList.org_id||
                       ' res_type_id='||changeList.org_id||
                       ' owner_type='||changeList.owner_type||
                       ' supplier_id='||changeList.supplier_id||
                       ' busi_type='||changeList.busi_type||
                       ' sub_busi_type='||changeList.sub_busi_type||
                       ' inout_flag='||changeList.inout_flag||
                       ' group_id='||changeList.group_id||
                       ' tel_segment='||changeList.tel_segment||
                       ' Amount='||changeList.Amount);
      select t.brand_id
        into p_brandId
        from im_res_type t
       where t.res_type_id = changeList.Res_Type_Id;
      select count(1)
        into v_sum
        from psi_stat_daypsi t
       where t.org_id = changeList.Org_Id
         and t.res_type_id = changeList.Res_Type_Id
         and t.owner_type = changeList.Owner_Type
         and t.supplier_id = changeList.Supplier_Id
         and t.stat_day = p_statDay
         and (t.group_id = changeList.Group_Id or
              (t.group_id is null and changeList.Group_Id is null)
              )
         and (t.tel_segment = changeList.Tel_Segment or
              (t.tel_segment is null and changeList.Tel_Segment is null)
              );
    dbms_output.put_line('-->changeList循环：p_brandId='||p_brandId);
    dbms_output.put_line('-->changeList循环：v_sum='||v_sum);
      if v_sum = 0 then
        insert into psi_stat_daypsi
          (REGION,
           ORG_ID,
           RES_KIND_ID,
           STAT_DAY,
           STAT_TYPE,
           BRAND_ID,
           RES_TYPE_ID,
           SPECIAL_ATTR,
           OWNER_TYPE,
           SUPPLIER_ID,
           ORG_TYPE,
           ORIGINAL_NUM,
           LDSTORE_NUM,
           REFUND_NUM,
           ASIGIN_NUM,
           ASIGOUT_NUM,
           SALE_NUM,
           SALEBACK_NUM,
           ADJUSTIN_NUM,
           ADJUSTOUT_NUM,
           EXCHGIN_NUM,
           EXCHGOUT_NUM,
           STOCK_NUM,
           CREATE_DATE,
           GROUP_ID,
           TEL_SEGMENT
           )
        values
          (p_region,
           changeList.Org_Id,
           p_resKindId,
           p_statDay,
           p_statType,
           p_brandId,
           changeList.Res_Type_Id,
           p_specialAttr,
           changeList.Owner_Type,
           changeList.Supplier_Id,
           changeList.Owner_Type,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           sysdate,
           changeList.group_id,
           changeList.tel_segment
           );
      end if;
      --入库数量：入库数量为业务类型为入库，但是子业务类型不为盘盈（入库）的数据
      if changeList.Busi_Type = 'LDSTORE' and changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.LDSTORE_NUM = t.LDSTORE_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id

           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：入库数量，当Busi_Type= LDSTORE 且Inout_Flag= IN 时更新psi_stat_daypsi表数据');
       --退库数量：退库数量为业务类型为退库，但是子业务类型不为盘亏（遗失退库）或盘亏（报废）的数据。
      elsif changeList.Busi_Type = 'ROLLBACK' and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.REFUND_NUM = t.REFUND_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：退库数量，当Busi_Type= ROLLBACK 且Inout_Flag= OUT 时更新psi_stat_daypsi表数据');
        --调入数量：调入数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为入库的数据。
      elsif changeList.Busi_Type IN ('ASIGOUT','ASIGIN') and
            changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.ASIGIN_NUM = t.ASIGIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：调入数量，当Busi_Type= ASIGOUT 或者 ASIGIN 且Inout_Flag= IN 时更新psi_stat_daypsi表数据');
      --调出数量：调出数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为出库的数据。
      elsif changeList.Busi_Type IN ('ASIGOUT','ASIGIN') and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.ASIGOUT_NUM = t.ASIGOUT_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：调出数量，当Busi_Type= ASIGOUT 或者 ASIGIN 且Inout_Flag= OUT 时更新psi_stat_daypsi表数据');
        --销售数量：销售数量为业务类型为销售的数据。
      elsif changeList.Busi_Type = 'SALE' and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.SALE_NUM = t.SALE_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：销售数量，当Busi_Type= SALE 且Inout_Flag= OUT 时更新psi_stat_daypsi表数据');
       --销售回退数量：销售回退数量为业务类型为销售回退的数据。
      elsif changeList.Busi_Type = 'SALEBACK' and
            changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.SALEBACK_NUM = t.SALEBACK_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：销售回退数量，当Busi_Type= SALEBACK 且Inout_Flag= IN 时更新psi_stat_daypsi表数据');
        --换货入库数量：换货入库数量为业务类型为换货，出入库标志为入库的数据。
      elsif changeList.Busi_Type = 'EXCHGIN' and
            changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.EXCHGIN_NUM = t.EXCHGIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：换货入库数量，当Busi_Type= EXCHGIN 且Inout_Flag= IN 时更新psi_stat_daypsi表数据');
      --换货出库数量：换货出库数量为业务类型为换货，出入库标志为出库的数据。
      elsif changeList.Busi_Type = 'EXCHGOUT' and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.EXCHGOUT_NUM = t.EXCHGOUT_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and (t.group_id = changeList.group_id or
                (t.group_id is null and changeList.Group_Id is null))
           and (t.tel_segment = changeList.tel_segment or
                (t.tel_segment is null and changeList.Tel_Segment is null))
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->changeList循环：换货出库数量，当Busi_Type= EXCHGOUT 且Inout_Flag= OUT 时更新psi_stat_daypsi表数据');
      end if;
    end;
  end loop;
  commit; --处理完一个单位的数据后，提交
  ----删除所有数量都为 0 的记录
  delete from psi_stat_daypsi  t where
t.original_num + t.stock_num + t.ldstore_num +t.refund_num + t.asigin_num
+ t.asigout_num + t.sale_num + t.saleback_num + t.adjustin_num + t.adjustout_num +  t.exchgin_num + t.exchgout_num = 0
and t.stat_day = p_statDay ;
commit;
end;
/

CREATE OR REPLACE PROCEDURE "AP_RESPSI_STOCKAGE" IS
  v_amount        number; --记录库存数量
  v_avg_age       number; --平均库龄
  v_curDay        date; --当前日期
  v_lowLimit1      number; --区间下限
  v_upLimit1       number; --区间上限
  v_lowLimit2      number; --区间下限
  v_upLimit2       number; --区间上限
  v_lowLimit3      number; --区间下限
  v_upLimit3       number; --区间上限
  v_lowLimit4      number; --区间下限
  v_upLimit4       number; --区间上限
  v_lowLimit5      number; --区间下限
  v_upLimit5       varchar2(32); --区间上限
  v_Stkage_phase1 varchar2(32);
  v_Stkage_phase2 varchar2(32);
  v_Stkage_phase3 varchar2(32);
  v_Stkage_phase4 varchar2(32);
  v_Stkage_phase5 varchar2(32);
  v_Stkage_num1   number;
  v_Stkage_num2   number;
  v_Stkage_num3   number;
  v_Stkage_num4   number;
  v_Stkage_num5   number;
  v_stock_num number;
begin
  v_Stkage_num1 := 0;
  v_Stkage_num2 := 0;
  v_Stkage_num3 := 0;
  v_Stkage_num4 := 0;
  v_Stkage_num5 := 0;
  v_stock_num :=0;
  delete from psi_stk_stockage_tmp;
  select sysdate into v_curDay from dual;
  dbms_output.put_line('-->v_curDay='||v_curDay);
  --1.统计日志表记录状态为PENDING
  insert into Psi_stat_stkagelog
    (Region, Static_Day, Status, Status_Date)
  values
    ('634', to_char(sysdate, 'YYYYMMDD'), 'PENDING', v_curDay);
 ---获取各个区间
  select t.dict_name, t.description
            into v_lowLimit1, v_upLimit1
            from psi_dict_item t
           where t.dict_id = '1'
             and t.group_id = 'date_limit';
          v_Stkage_phase1 := '[' || v_lowLimit1 || ',' || v_upLimit1 || ']';
   dbms_output.put_line('-->v_Stkage_phase1='||v_Stkage_phase1);
   select t.dict_name, t.description
            into v_lowLimit2, v_upLimit2
            from psi_dict_item t
           where t.dict_id = '2'
             and t.group_id = 'date_limit';
          v_Stkage_phase2 := '[' || v_lowLimit2 || ',' || v_upLimit2 || ']';
    dbms_output.put_line('-->v_Stkage_phase2='||v_Stkage_phase2);
    select t.dict_name, t.description
            into v_lowLimit3, v_upLimit3
            from psi_dict_item t
           where t.dict_id = '3'
             and t.group_id = 'date_limit';
          v_Stkage_phase3 := '[' || v_lowLimit3 || ',' || v_upLimit3 || ']';
    dbms_output.put_line('-->v_Stkage_phase3='||v_Stkage_phase3);
    select t.dict_name, t.description
            into v_lowLimit4, v_upLimit4
            from psi_dict_item t
           where t.dict_id = '4'
             and t.group_id = 'date_limit';
          v_Stkage_phase4 := '[' || v_lowLimit4 || ',' || v_upLimit4 || ']';
  dbms_output.put_line('-->v_Stkage_phase4='||v_Stkage_phase4);
  select t.dict_name,t.description
            into v_lowLimit5,v_upLimit5
            from psi_dict_item t
           where t.dict_id = '5'
             and t.group_id = 'date_limit';
          v_Stkage_phase5 := '[' || v_lowLimit5 || ',' || v_upLimit5 || ']';
  dbms_output.put_line('-->v_Stkage_phase5='||v_Stkage_phase5);


  --2.循环实时库存表，取出单位，机型，settle_mode,库存量
  for c_in in (select t.region,
                      t.org_id,
                      t.res_type_id,
                      t.res_kind_id,
                      t.settle_mode,
                      sum(t.stock_num) stock_num
                 from PSI_STK_REALTIME_MSTOCK t
                where t.busi_status = 'USABLE'
                and  t.inv_status = 'INSTORE'
                and t.res_kind_id = 'rsclM'
                group by t.region,
                         t.res_kind_id,
                         t.org_id,
                         t.res_type_id,
                         settle_mode
               having sum(t.stock_num) > 0) loop
    begin
    dbms_output.put_line('-->c_in循环：region='||c_in.region||
                    ' org_id='||c_in.org_id||
                    ' res_type_id='||c_in.res_type_id||
                    ' res_kind_id='||c_in.res_kind_id||
                    ' settle_mode='||c_in.settle_mode||
                    ' stock_num='||c_in.stock_num);
      v_avg_age := 0;
      v_amount  := 0;
      v_stock_num := 0;
      v_Stkage_num1 := 0;
      v_Stkage_num2 := 0;
      v_Stkage_num3 := 0;
      v_Stkage_num4 := 0;
      v_Stkage_num5 := 0;
      --3.计算平均库龄
        for c_in1 in (select t.status_date
                        from im_inv_mobtel t
                       where t.busi_status in ('USABLE','KEEP')
                         and t.inv_status = 'INSTORE'
                         and t.org_id = c_in.org_id
                         and t.settle_mode = c_in.settle_mode
                         and t.res_type_id = c_in.res_type_id
                       order by t.status_date desc) loop
        begin
      dbms_output.put_line('-->c_in1循环：status_date='||c_in1.status_date);
          v_stock_num := v_stock_num+1;
          -----IMEI号在库时间
          v_amount  := v_amount +(to_date('20201103','yyyy-mm-dd') - c_in1.status_date);
          dbms_output.put_line('-->c_in1循环：v_stock_num='||v_stock_num);
          dbms_output.put_line('-->c_in1循环：v_amount='||v_amount);
          ------计算分段库龄
           if (sysdate - c_in1.status_date) >= v_lowLimit1 and (sysdate - c_in1.status_date) < v_upLimit1  then
              v_Stkage_num1    :=  v_Stkage_num1 +1;
        dbms_output.put_line('-->计算分段库龄：v_Stkage_num1='||v_Stkage_num1);
            end if;

           if (sysdate - c_in1.status_date) >= (v_lowLimit2 - 1) and (sysdate - c_in1.status_date) < v_upLimit2  then
              v_Stkage_num2    :=  v_Stkage_num2 +1;
        dbms_output.put_line('-->计算分段库龄：v_Stkage_num2='||v_Stkage_num2);
            end if;

           if (sysdate - c_in1.status_date) >= (v_lowLimit3 - 1) and (sysdate - c_in1.status_date) < v_upLimit3  then
              v_Stkage_num3    :=  v_Stkage_num3 +1;
        dbms_output.put_line('-->计算分段库龄：v_Stkage_num3='||v_Stkage_num3);
            end if;

           if (sysdate - c_in1.status_date) >= (v_lowLimit4 - 1) and (sysdate - c_in1.status_date) < v_upLimit4  then
              v_Stkage_num4    :=  v_Stkage_num4 +1;
        dbms_output.put_line('-->计算分段库龄：v_Stkage_num4='||v_Stkage_num4);
            end if;
     ---- 阶段5下限设置为... 时，表示无穷
           if v_upLimit5 = '...' then
      if (sysdate - c_in1.status_date) >= (v_lowLimit5 - 1) then
      v_Stkage_num5    :=  v_Stkage_num5 +1;
      dbms_output.put_line('--> 阶段5下限设置为... 时：v_Stkage_num5='||v_Stkage_num5);
    end if;
    else
      if (sysdate - c_in1.status_date) >= (v_lowLimit5 - 1) and (sysdate - c_in1.status_date) < v_upLimit5  then
        v_Stkage_num4    :=  v_Stkage_num4 +1;
      end if;
     end if;

        end;
        end loop;
    dbms_output.put_line('-->循环结束后：v_stock_num='||v_stock_num);
            ---------相同机型所有IMEI号在库时间/库存量 = 平均库龄
           if v_stock_num >0 then
           v_avg_age := v_amount/v_stock_num;
       dbms_output.put_line('-->相同机型时v_avg_age='||v_avg_age);
           end if;
      insert into psi_stk_stockage_tmp
        (REGION,
         ORG_ID,
         RES_KIND_ID,
         RES_TYPE_ID,
         SETTLE_MODE,
         STOCK_NUM,
         AVERAGE_AGE,
         STKAGE_PHASE1,
         STKAGE_NUM1,
         STKAGE_PHASE2,
         STKAGE_NUM2,
         STKAGE_PHASE3,
         STKAGE_NUM3,
         STKAGE_PHASE4,
         STKAGE_NUM4,
         STKAGE_PHASE5,
         STKAGE_NUM5,
         STATIC_DAY,
         STATIC_DATE)
      values
        (c_in.region,
         c_in.org_id,
         c_in.res_kind_id,
         c_in.res_type_id,
         c_in.settle_mode,
         v_stock_num,
         v_avg_age,
         v_Stkage_phase1,
         v_Stkage_num1,
         v_Stkage_phase2,
         v_Stkage_num2,
         v_Stkage_phase3,
         v_Stkage_num3,
         v_Stkage_phase4,
         v_Stkage_num4,
         v_Stkage_phase5,
         v_Stkage_num5,
         to_char(sysdate, 'YYYYMMDD'),
         sysdate);
         commit;
    end;
  end loop;

--------更新库龄统计日志表
  update Psi_stat_stkagelog t
     set t.status = 'CLOSE', t.status_date = sysdate
   where t.status_date = v_curDay;

 --------库龄表中数据移入历史
  insert into psi_stk_stockage_his

    (REGION,
     ORG_ID,
     RES_KIND_ID,
     RES_TYPE_ID,
     SETTLE_MODE,
     STOCK_NUM,
     AVERAGE_AGE,
     STKAGE_PHASE1,
     STKAGE_NUM1,
     STKAGE_PHASE2,
     STKAGE_NUM2,
     STKAGE_PHASE3,
     STKAGE_NUM3,
     STKAGE_PHASE4,
     STKAGE_NUM4,
     STKAGE_PHASE5,
     STKAGE_NUM5,
     STATIC_DAY,
     STATIC_DATE)
    select REGION,
           ORG_ID,
           RES_KIND_ID,
           RES_TYPE_ID,
           SETTLE_MODE,
           STOCK_NUM,
           AVERAGE_AGE,
           STKAGE_PHASE1,
           STKAGE_NUM1,
           STKAGE_PHASE2,
           STKAGE_NUM2,
           STKAGE_PHASE3,
           STKAGE_NUM3,
           STKAGE_PHASE4,
           STKAGE_NUM4,
           STKAGE_PHASE5,
           STKAGE_NUM5,
           STATIC_DAY,
           STATIC_DATE
      from psi_stk_stockage;

   delete from psi_stk_stockage t;

   --临时表统计的数据移入库龄统计表
  insert into psi_stk_stockage
    (REGION,
     ORG_ID,
     RES_KIND_ID,
     RES_TYPE_ID,
     SETTLE_MODE,
     STOCK_NUM,
     AVERAGE_AGE,
     STKAGE_PHASE1,
     STKAGE_NUM1,
     STKAGE_PHASE2,
     STKAGE_NUM2,
     STKAGE_PHASE3,
     STKAGE_NUM3,
     STKAGE_PHASE4,
     STKAGE_NUM4,
     STKAGE_PHASE5,
     STKAGE_NUM5,
     STATIC_DAY,
     STATIC_DATE)
    select REGION,
           ORG_ID,
           RES_KIND_ID,
           RES_TYPE_ID,
           SETTLE_MODE,
           STOCK_NUM,
           AVERAGE_AGE,
           STKAGE_PHASE1,
           STKAGE_NUM1,
           STKAGE_PHASE2,
           STKAGE_NUM2,
           STKAGE_PHASE3,
           STKAGE_NUM3,
           STKAGE_PHASE4,
           STKAGE_NUM4,
           STKAGE_PHASE5,
           STKAGE_NUM5,
           STATIC_DAY,
           STATIC_DATE
      from psi_stk_stockage_tmp;
  commit;
delete from psi_stk_stockage t
   where t.stock_num = 0
     and t.stkage_num1 = 0
     and t.stkage_num2 = 0
     and t.stkage_num3 = 0
     and t.stkage_num4 = 0
     and t.stkage_num5 = 0;
  commit;
end AP_RESPSI_STOCKAGE;
/

CREATE OR REPLACE PROCEDURE "LOC_AGENTCHANNEL_NUM_PRC" is
  var_region     varchar2(18);
  var_regionname varchar2(128);
  var_orgid      varchar2(32);
  var_orgname    varchar2(200);
  var_countnum   varchar2(32);

  Cursor mycursor is
    SELECT T1.REGION REGION,
           T2.REGIONNAME REGIONNAME,
           SUBSTR(T1.ORGAID, 0, 8) ORGAID,
           T3.ORGNAME ORGNAME,
           COUNT(1) COUNTNUM
      FROM (SELECT DISTINCT A.REGION, D.ORGAID
              FROM ICDPUB.T_UCP_STAFFBASICINFO A,
                   ICDPUB.T_UCP_STAFFROLE      B,
                   ICDPUB.T_UCP_ROLEAUTH       C,
                   ICDPUB.T_UCP_ORGAINFO       D
             WHERE A.STAFFID = B.STAFFID
               AND B.ROLEID = C.ROLEID
               AND A.ORGAID = D.ORGAID
               AND C.AUTHID = 'RESPSI_MobileStock'
               AND D.ORGASTATE = '0'
               AND A.STAFFSTATE = '1'
               AND A.STAFFIDSTATUS = '01'
               AND EXISTS
             (SELECT 1
                      FROM AGENT E
                     WHERE E.AGENTID = D.ORGAID
                       AND E.AGENTTYPE IN
                           (SELECT DICTID
                              FROM DICT_ITEM
                             WHERE GROUPID = 'LocAgentStoreType'))
            UNION
            SELECT DISTINCT A.REGION, D.ORGAID
              FROM ICDPUB.T_UCP_STAFFBASICINFO A,
                   ICDPUB.T_UCP_USERAUTH       B,
                   ICDPUB.T_UCP_ORGAINFO       D
             WHERE A.STAFFID = B.STAFFID
               AND A.ORGAID = D.ORGAID
               AND B.AUTHID = 'RESPSI_MobileStock'
               AND D.ORGASTATE = '0'
               AND A.STAFFSTATE = '1'
               AND A.STAFFIDSTATUS = '01'
               AND EXISTS
             (SELECT 1
                      FROM AGENT E
                     WHERE E.AGENTID = D.ORGAID
                       AND E.AGENTTYPE IN
                           (SELECT DICTID
                              FROM DICT_ITEM
                             WHERE GROUPID = 'LocAgentStoreType'))) T1,
           REGION_LIST T2,
           ORGANIZATION T3
     WHERE T1.REGION = T2.REGION
       AND SUBSTR(T1.ORGAID, 0, 8) = T3.ORGID
     GROUP BY T1.REGION, T2.REGIONNAME, SUBSTR(T1.ORGAID, 0, 8), T3.ORGNAME;

begin
  open mycursor;
  loop
    fetch mycursor
      into var_region, var_regionname, var_orgid, var_orgname, var_countnum; --相当于foreach
    exit when mycursor%notfound; --判断如果没有找到
  dbms_output.put_line('-->loop循环：var_region='|| var_region||
                   ' var_regionname='||var_regionname||
                   ' var_orgid='||var_orgid||
                   ' var_orgname='||var_orgname||
                   ' var_countnum='||var_countnum);
    begin
      insert into loc_agentchannel_num
        (region, regionname, orgid, orgname, countnum)
      values
        (var_region, var_regionname, var_orgid, var_orgname, var_countnum);
   dbms_output.put_line('-->loop循环：loc_agentchannel_num 表数据插入成功');
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        null;
    end;
  end loop;
  close mycursor; --关闭游标
end loc_agentchannel_num_prc;
/

CREATE OR REPLACE PROCEDURE "LOC_RANKLIST_DAYPSI" is
begin
  --删除排行表中当前数据
  delete from psi_info_mob_rankinglist t where t.ranking_type = 'sellRank';
  ---统计最近30日提货数据
  for v_record in (select tt.amt,
                          tt.res_type_id,
                          (select n.brand_id
                             from im_res_type n
                            where n.res_type_id = tt.res_type_id
                              and rownum = 1) brand_id
                     from (select t.amt, t.res_type_id
                             from (select sum(b.sale_num) amt, b.res_type_id
                                     from psi_stat_daypsi b
                                    where b.create_date > sysdate - 30
                                      and b.stat_day >
                                          to_char(sysdate - 30, 'yyyymmdd')
                                    group by b.res_type_id) t
                            order by t.amt desc) tt
                    where rownum <= 30) loop
  dbms_output.put_line('-->v_record循环：amt='||v_record.amt||
                     ' res_type_id='||v_record.res_type_id||
                     ' brand_id='||v_record.brand_id);
    insert into psi_info_mob_rankinglist
      (Ranking_type,
       Res_type_id,
       Brand_id,
       Static_amt,
       Static_amt1,
       Static_amt2,
       Static_amt3,
       Org_id)
    values
      ('sellRank',
       v_record.res_type_id,
       v_record.brand_id,
       v_record.amt,
       null,
       null,
       null,
       null);
    commit;
  dbms_output.put_line('--> 表 psi_info_mob_rankinglist 数据插入成功');
  end loop;
  commit;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end loc_ranklist_daypsi;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_DAYPSI_NEW0505" IS
  p_region      number(5);
  p_orgId       varchar2(32);
  p_resKindId   varchar2(32) := 'rsclM';
  p_resTypeId   varchar2(32);
  p_brandId     varchar2(32);
  p_specialAttr varchar2(32);
  p_isBak varchar2(32);
  p_ownType     varchar2(32);
  p_supplierId  varchar2(32);
  p_settleMode  varchar2(32);
  p_statDay     varchar2(32) := to_char(to_date('20301019','yyyy-mm-dd') -1,'YYYYMMDD');
  p_statType    varchar2(32) := 'COMMON';
  p_departCode  varchar2(32);
  v_stockNum    number(10);
  v_sum         number(10);
begin
  -------首先删除昨天的数据
  delete from psi_stat_daypsi t
   where t.stat_day = p_statDay
     and t.res_kind_id = 'rsclM'
     and t.stat_type = 'COMMON';
  commit;
  dbms_output.put_line('-->删除昨天的数据');
  -----v_msg:= '删除' || p_statDay || '日报数据' || sql%rowcount || '成功';
  -----insert into z_msg values(v_msg,sysdate);
  commit;
  ---------初始化当天日报
  begin
    insert into psi_stat_daypsi
      (REGION,
       ORG_ID,
       RES_KIND_ID,
       STAT_DAY,
       STAT_TYPE,
       BRAND_ID,
       RES_TYPE_ID,
       SPECIAL_ATTR,
       OWNER_TYPE,
       SUPPLIER_ID,
       ORG_TYPE,
       ORIGINAL_NUM,
       LDSTORE_NUM,
       REFUND_NUM,
       ASIGIN_NUM,
       ASIGOUT_NUM,
       SALE_NUM,
       SALEBACK_NUM,
       ADJUSTIN_NUM,
       ADJUSTOUT_NUM,
       EXCHGIN_NUM,
       EXCHGOUT_NUM,
       STOCK_NUM,
       CREATE_DATE,
       SETTLE_MODE,
       DEPARTCODE)
      (select REGION,
              ORG_ID,
              RES_KIND_ID,
              p_statDay,
              STAT_TYPE,
              BRAND_ID,
              RES_TYPE_ID,
              SPECIAL_ATTR,
              OWNER_TYPE,
              SUPPLIER_ID,
              OWNER_TYPE,
              STOCK_NUM,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              sysdate,
              SETTLE_MODE,
              DEPARTCODE
         from PSI_STAT_DAYPSI a
        where a.res_kind_id = p_resKindId
        and a.stat_day =
              to_char((to_date(p_statDay, 'YYYYMMDD')-1), 'YYYYMMDD'));
    commit;
  dbms_output.put_line('-->初始化当天日报');
  end;
  commit;
  --循环处理各个单位的数据
  for invList in (select t.region,
       t.res_kind_id,
       t.org_id,
       t.res_type_id,
       t.supplier_id,
       t.settle_mode,
       t.owner_type,
       t.special_attr,
       t.is_bak,
       sum(t.stock_num) countNum
  from psi_STK_REALTIME_MSTOCK t
 where t.res_kind_id = 'rsclM'
 group by t.region,
          t.res_kind_id,
          t.org_id,
          t.res_type_id,
          t.supplier_id,
          t.settle_mode,
          t.owner_type,
          t.special_attr,
          t.settle_mode,
          t.is_bak) loop
    begin
      dbms_output.put_line('-->开始循环：invList');
      p_orgId      := invList.org_id;
      p_resTypeId  := invList.res_type_id;
      p_ownType    := invList.owner_Type;
      p_supplierId := invList.supplier_Id;
      p_settleMode := invList.settle_mode;
      p_region     := invList.region;
      p_resKindId  := 'rsclM';
      p_specialAttr:= invList.Special_Attr;
      p_isBak      := invList.Is_Bak;
      v_stockNum   := invList.countNum;
    dbms_output.put_line('-->invList循环：p_orgId='||p_orgId||
                      ' p_resTypeId='||p_resTypeId||
                      ' p_ownType='||p_ownType||
                      ' p_supplierId='||p_supplierId||
                      ' p_settleMode='||p_settleMode||
                      ' p_region='||p_region||
                      ' p_resKindId='||p_resKindId||
                      ' p_specialAttr='||p_specialAttr||
                      ' p_isBak='||p_isBak||
                      ' v_stockNum='||v_stockNum);
      select t.brand_id
        into p_brandId
        from im_res_type t
       where t.res_type_id = p_resTypeId;
     dbms_output.put_line('-->invList：p_brandId=' || p_brandId);
      select count(1)
        into v_sum
        from psi_stat_daypsi t
       where t.region = p_region
         and t.org_id = p_orgId
         and t.res_kind_id = p_resKindId
         and t.stat_day = p_statDay
         and t.res_type_id = p_resTypeId
         and t.owner_type = p_ownType
         and t.supplier_id = p_supplierId
         and t.settle_mode = p_settleMode
         and t.special_attr = p_specialAttr
         and t.is_bak = p_isBak;
     dbms_output.put_line('-->invList：v_sum=' || v_sum);
      if v_sum > 0 then
        update psi_stat_daypsi t
           set t.stock_num = v_stockNum
         where t.region = p_region
           and t.org_id = p_orgId
           and t.res_kind_id = p_resKindId
           and t.stat_day = p_statDay
           and t.res_type_id = p_resTypeId
           and t.owner_type = p_ownType
           and t.supplier_id = p_supplierId
           and t.settle_mode = p_settleMode;
       dbms_output.put_line('-->invList：表数据psi_stat_daypsi更新成功');
        commit;
      else
        --数据插入
        INSERT INTO psi_stat_daypsi
          (REGION,
           ORG_ID,
           RES_KIND_ID,
           STAT_DAY,
           STAT_TYPE,
           BRAND_ID,
           RES_TYPE_ID,
           SPECIAL_ATTR,
           OWNER_TYPE,
           SUPPLIER_ID,
           ORG_TYPE,
           ORIGINAL_NUM,
           LDSTORE_NUM,
           REFUND_NUM,
           ASIGIN_NUM,
           ASIGOUT_NUM,
           SALE_NUM,
           SALEBACK_NUM,
           ADJUSTIN_NUM,
           ADJUSTOUT_NUM,
           EXCHGIN_NUM,
           EXCHGOUT_NUM,
           STOCK_NUM,
           CREATE_DATE,
           SETTLE_MODE)
        VALUES
          (p_region,
           p_orgId,
           p_resKindId,
           p_statDay,
           p_statType,
           p_brandId,
           p_resTypeId,
           p_specialAttr,
           p_ownType,
           p_supplierId,
           p_ownType,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           v_stockNum,
           sysdate,
           p_settleMode);
        commit;
    dbms_output.put_line('-->invList：表数据psi_stat_daypsi插入成功');
      end if;
      commit;
    end;
  end loop;

  for changeList in (select org_id,
                            res_type_id,
                            owner_type,
                            supplier_id,
                            settle_mode,
                            busi_type,
                            sub_busi_type,
                            inout_flag,
                            sum(amount) Amount
                       from psi_busi_stock_log_his t
                      where t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                        and t.create_date <
                            to_date(p_statDay, 'YYYYMMDD')+1
                        and t.busi_type not in ('PICK', 'UNPICK', 'ASIGOUT')
                        and (t.is_bak = 'NO' or t.is_bak is null)
                        and t.res_kind_id = p_resKindId
                      group by org_id,
                               res_type_id,
                               owner_type,
                               supplier_id,
                               settle_mode,
                               busi_type,
                               sub_busi_type,
                               inout_flag) loop
    begin
    dbms_output.put_line('-->开始for循环：changeList');
    dbms_output.put_line('-->changeList；org_id' || changeList.org_id);
    dbms_output.put_line('-->changeList；res_type_id' || changeList.res_type_id);
      select t.brand_id
        into p_brandId
        from im_res_type t
       where t.res_type_id = changeList.Res_Type_Id;
     dbms_output.put_line('-->changeList：p_brandId=' || p_brandId);
      select count(1)
        into v_sum
        from psi_stat_daypsi t
       where t.org_id = changeList.Org_Id
         and t.res_type_id = changeList.Res_Type_Id
         and t.owner_type = changeList.Owner_Type
         and t.supplier_id = changeList.Supplier_Id
         and t.settle_mode = changeList.Settle_Mode
         and t.stat_day = p_statDay;
     dbms_output.put_line('-->changeList：v_sum=' || v_sum);

      if v_sum = 0 then
        insert into psi_stat_daypsi
          (REGION,
           ORG_ID,
           RES_KIND_ID,
           STAT_DAY,
           STAT_TYPE,
           BRAND_ID,
           RES_TYPE_ID,
           SPECIAL_ATTR,
           OWNER_TYPE,
           SUPPLIER_ID,
           ORG_TYPE,
           ORIGINAL_NUM,
           LDSTORE_NUM,
           REFUND_NUM,
           ASIGIN_NUM,
           ASIGOUT_NUM,
           SALE_NUM,
           SALEBACK_NUM,
           ADJUSTIN_NUM,
           ADJUSTOUT_NUM,
           EXCHGIN_NUM,
           EXCHGOUT_NUM,
           STOCK_NUM,
           CREATE_DATE,
           SETTLE_MODE)
        values
          (p_region,
           changeList.Org_Id,
           p_resKindId,
           p_statDay,
           p_statType,
           p_brandId,
           changeList.Res_Type_Id,
           p_specialAttr,
           changeList.Owner_Type,
           changeList.Supplier_Id,
           changeList.Owner_Type,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           sysdate,
           changeList.Settle_Mode);
       dbms_output.put_line('-->changeList：表数据插入成功');
      end if;
      dbms_output.put_line('-->changeList：Busi_Type=' || changeList.Busi_Type);
      dbms_output.put_line('-->changeList：Inout_Flag=' || changeList.Inout_Flag);
      dbms_output.put_line('-->changeList：sub_busi_type=' || changeList.sub_busi_type);
      ---普通入库数量
      if changeList.Busi_Type = 'LDSTORE' and changeList.Inout_Flag = 'IN' and
         changeList.sub_busi_type != 'CHECK_MORE_LDSTORE' then
        update psi_stat_daypsi t
           set t.LDSTORE_NUM = t.LDSTORE_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
     dbms_output.put_line('-->1changeList：表psi_stat_daypsi数据更新');
        ----盘点入库数量
      elsif changeList.Busi_Type = 'LDSTORE' and
            changeList.Inout_Flag = 'IN' and
            changeList.sub_busi_type = 'CHECK_MORE_LDSTORE' then
        update psi_stat_daypsi t
           set t.ADJUSTIN_NUM = t.ADJUSTIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
     dbms_output.put_line('-->2changeList：表psi_stat_daypsi数据更新');
        ----退库数量
      elsif changeList.Busi_Type = 'WITHDRAW' and
            changeList.Inout_Flag = 'OUT' and
            changeList.sub_busi_type != 'CHECK_LESS_LOSS' and
            changeList.sub_busi_type != 'CHECK_LESS_DISCARD' then
        update psi_stat_daypsi t
           set t.REFUND_NUM = t.REFUND_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->3changeList：表psi_stat_daypsi数据更新');
        ----调入数量  不包括盘盈（调拨） --代理商买断记录为调拨入
      elsif changeList.Busi_Type in ('ASIGIN', 'SALE2AGENT') and
            changeList.Inout_Flag = 'IN' and
            changeList.sub_busi_type != 'CHECK_MORE_ASIG' then
        update psi_stat_daypsi t
           set t.ASIGIN_NUM = t.ASIGIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->4changeList：表psi_stat_daypsi数据更新');
        ----调出数量 CHECK_MORE_ASIG
      elsif changeList.Busi_Type = 'ASIGIN' and
            changeList.Inout_Flag = 'OUT' and
            changeList.sub_busi_type != 'CHECK_MORE_ASIG' then
        update psi_stat_daypsi t
           set t.ASIGOUT_NUM = t.ASIGOUT_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->5changeList：表psi_stat_daypsi数据更新');
        -------销售数量
      elsif changeList.Busi_Type in ('SALE', 'SALE2AGENT') and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.SALE_NUM = t.SALE_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
       dbms_output.put_line('-->6changeList：表psi_stat_daypsi数据更新');

        -------销售回退数量
      elsif changeList.Busi_Type in ('SALE_ROLLBACK', 'UNSALE') and
            changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.SALEBACK_NUM = t.SALEBACK_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
     dbms_output.put_line('-->7changeList：表psi_stat_daypsi数据更新');
        ----------调整入库数量
      elsif changeList.Busi_Type in ('LDSTORE', 'ASIGOUT') and
            changeList.Inout_Flag = 'IN' and
            changeList.sub_busi_type in
            ('CHECK_MORE_LDSTORE', 'CHECK_MORE_ASIG', 'CHECK_MORE_ASIG') then
        update psi_stat_daypsi t
           set t.ADJUSTIN_NUM = t.ADJUSTIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
      dbms_output.put_line('-->8changeList：表psi_stat_daypsi数据更新');
        -----------调整出库数量
      elsif changeList.Busi_Type in ('WITHDRAW', 'ASIGOUT') and
            changeList.Inout_Flag = 'OUT' and
            changeList.sub_busi_type in
            ('CHECK_LESS_LOSS', 'CHECK_LESS_DISCARD', 'CHECK_MORE_ASIG') then
        update psi_stat_daypsi t
           set t.ADJUSTOUT_NUM = t.ADJUSTOUT_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
     dbms_output.put_line('--9changeList：表psi_stat_daypsi数据更新');
        ---------换货入
      elsif changeList.Busi_Type in ('EXCH4SALE', 'EXCH4AGENT') and
            changeList.Inout_Flag = 'IN' then
        update psi_stat_daypsi t
           set t.EXCHGIN_NUM = t.EXCHGIN_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
      dbms_output.put_line('-->10changeList：表psi_stat_daypsi数据更新');
        ---------换货出
      elsif changeList.Busi_Type in ('EXCH4SALE', 'EXCH4AGENT') and
            changeList.Inout_Flag = 'OUT' then
        update psi_stat_daypsi t
           set t.EXCHGOUT_NUM = t.EXCHGOUT_NUM + changeList.Amount
         where t.org_id = changeList.Org_Id
           and t.res_type_id = changeList.Res_Type_Id
           and t.owner_type = changeList.Owner_Type
           and t.supplier_id = changeList.Supplier_Id
           and t.settle_mode = changeList.Settle_Mode
           and t.stat_day = p_statDay;
     dbms_output.put_line('-->11changeList：表psi_stat_daypsi数据更新');
      end if;
    end;
  end loop;
  commit; --处理完一个单位的数据后，提交
  ----删除所有数量都为 0 的记录
  delete from psi_stat_daypsi t
   where t.original_num + t.stock_num + t.ldstore_num + t.refund_num +
         t.asigin_num + t.asigout_num + t.sale_num + t.saleback_num +
         t.adjustin_num + t.adjustout_num + t.exchgin_num + t.exchgout_num = 0
     and t.stat_day = p_statDay;
  commit;
end;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_DAYPSI_0414"
 IS
  p_region       number(5);
  p_orgId        varchar2(32);
  p_orgType      varchar2(32);
  p_resKindId    varchar2(32) := 'rsclM';
  p_resTypeId    varchar2(32);
  p_brandId      varchar2(32);
  p_specialAttr  varchar2(32);
  p_ownType      varchar2(32);
  p_supplierId   varchar2(32);
  p_statDay      varchar2(32) := to_char(to_date('20301019','yyyy-mm-dd') - 1, 'YYYYMMDD');
  p_statType     varchar2(32) := 'COMMON';
  v_originalNum  number(10);
  v_ldstoreNum   number(10);
  v_refundNum    number(10);
  v_asiginNum    number(10);
  v_asigoutNum   number(10);
  v_saleNum      number(10);
  v_salebackNum  number(10);
  v_adjustinNum  number(10);
  v_adjustoutNum number(10);
  v_exchginNum   number(10);
  v_exchgoutNum  number(10);
  v_stockNum     number(10);
begin
  --首先删除昨天的数据
  delete from PSI_STAT_DAYPSI t
   where t.stat_day = p_statDay
     and t.res_kind_id = p_resKindId
     and t.stat_type = p_statType;
  --循环处理各个地市的数据
  for regionList in (select region from region_list) loop
    begin
      p_region := regionList.region;
      DBMS_OUTPUT.put_line('-->regionList循环：p_region='||p_region || ' p_resKindId=' || p_resKindId || ' p_statDay=' || p_statDay);
      --循环处理各个单位的数据
      for orgList in (select distinct (t.org_id) orgId
                        from PSI_BUSI_STOCK_LOG_HIS t
                       where t.region = p_region
                         and t.res_kind_id = p_resKindId
                         and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                         and t.create_date <
                             to_date(p_statDay, 'YYYYMMDD') + 1) loop
        begin
          p_orgId := orgList.orgid;
          select t.ORGTYPE
            into p_orgType
            from organization t
           where t.ORGID = p_orgId;
          DBMS_OUTPUT.put_line('-->orgList循环：p_orgId='||p_orgId || ' p_orgType='|| p_orgType);
          --循环处理某一个单位下的数据
          for cycleList in (select t.res_type_id  resTypeId,
                                   t.special_attr specialAttr,
                                   t.owner_type   ownerType,
                                   t.supplier_id  supplierId
                              from PSI_BUSI_STOCK_LOG_HIS t
                             where t.region = p_region
                               and t.org_id = p_orgId
                               and t.res_kind_id = p_resKindId
                               and t.create_date >=
                                   to_date(p_statDay, 'YYYYMMDD')
                               and t.create_date <
                                   to_date(p_statDay, 'YYYYMMDD') + 1
                             group by t.res_type_id,
                                      t.special_attr,
                                      t.owner_type,
                                      t.supplier_id) loop
            begin
              p_resTypeId := cycleList.resTypeId;
              select t.brand_id
                into p_brandId
                from im_res_type t
               where t.res_type_id = p_resTypeId;
              p_specialAttr := cycleList.specialAttr;
              p_ownType     := cycleList.ownerType;
              p_supplierId  := cycleList.supplierId;
              DBMS_OUTPUT.put_line('-->一个循环');
              DBMS_OUTPUT.put_line('-->cycleList循环：p_resTypeId:' || p_resTypeId ||
                                   '  p_brandId:' || p_brandId ||
                                   '  p_specialAttr:' || p_specialAttr ||
                                   '  p_ownType:' || p_ownType ||
                                   '  p_supplierId:' || p_supplierId);
              --入库数量：入库数量为业务类型为入库，但是子业务类型不为盘盈（入库）的数据
              select nvl(sum(t.amount), 0)
                into v_ldstoreNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType                      -- owner_type有可能是null 20140122
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'LDSTORE'
                 and t.sub_busi_type != 'panying';
         dbms_output.put_line('-->入库数量：v_ldstoreNum='||v_ldstoreNum);
              --退库数量：退库数量为业务类型为退库，但是子业务类型不为盘亏（遗失退库）或盘亏（报废）的数据。
              select nvl(sum(t.amount), 0)
                into v_refundNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'WITHDRAW'
                 and t.sub_busi_type != 'pankui';
         dbms_output.put_line('-->退库数量：v_refundNum='||v_refundNum);
              --调入数量：调入数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_asiginNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'ASIGOUT'
                 and t.sub_busi_type != 'panying1';
         dbms_output.put_line('-->调入数量：v_asiginNum='||v_asiginNum);
              --调出数量：调出数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_asigoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'ASIGOUT'
                 and t.sub_busi_type != 'pankui1';
         dbms_output.put_line('-->调出数量：v_asigoutNum='||v_asigoutNum);
              --销售数量：销售数量为业务类型为销售的数据。
              select nvl(sum(t.amount), 0)
                into v_saleNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'SALE';
         dbms_output.put_line('-->销售数量：v_saleNum='||v_saleNum);
              --销售回退数量：销售回退数量为业务类型为销售回退的数据。
              select nvl(sum(t.amount), 0)
                into v_salebackNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'SALEBACK';
         dbms_output.put_line('-->销售回退数量：v_salebackNum='||v_salebackNum);
              --调整入库数量：调整入库数量为业务类型为入库，子业务类型为盘盈（入库）的数据 或 业务类型为调拨，子业务类型为盘盈（调拨）出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_adjustinNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type in ('LDSTORE', 'ASIGOUT')
                 and t.sub_busi_type in ('panying', 'panying1');
         dbms_output.put_line('-->调整入库数量：v_adjustinNum='||v_adjustinNum);
              --调整出库数量：调整出库数量为业务类型为退库，子业务类型为盘亏（遗失退库）或盘亏（报废）的数据 或业务类型为调拨，子业务类型为盘盈（调拨）出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_adjustoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type in ('WITHDRAW', 'ASIGOUT')
                 and t.sub_busi_type in ('pankui', 'pankui1');
         dbms_output.put_line('-->调整出库数量：v_adjustoutNum='||v_adjustoutNum);
              --换货入库数量：换货入库数量为业务类型为换货，出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_exchginNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'EXCHGIN';
         dbms_output.put_line('-->换货入库数量：v_exchginNum='||v_exchginNum);
              --换货出库数量：换货出库数量为业务类型为换货，出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_exchgoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'EXCHGOUT';
        dbms_output.put_line('-->换货出库数量：v_exchgoutNum='||v_exchgoutNum);
              --前一天数量
              select nvl(sum(t.stock_num), 0)
                into v_originalNum
                from PSI_STAT_DAYPSI t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.stat_day = to_char(sysdate - 2, 'YYYYMMDD')
                 and t.stat_type = p_statType
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
        dbms_output.put_line('-->前一天数量：v_originalNum='||v_originalNum);
              --最新库存量
              select nvl(sum(t.stock_num), 0)
                into v_stockNum
                from PSI_STK_REALTIME_MSTOCK t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and t.inv_status = 'INSTORE'
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
         dbms_output.put_line('-->最新库存量：v_stockNum='||v_stockNum);
              if (v_ldstoreNum != 0 or v_refundNum != 0 or v_asiginNum != 0 or
                 v_asigoutNum != 0 or v_adjustinNum != 0 or
                 v_adjustoutNum != 0 or v_exchginNum != 0 or
                 v_exchgoutNum != 0) then
                begin
                  --数据插入
                  INSERT INTO PSI_STAT_DAYPSI
                    (REGION,
                     ORG_ID,
                     RES_KIND_ID,
                     STAT_DAY,
                     STAT_TYPE,
                     BRAND_ID,
                     RES_TYPE_ID,
                     SPECIAL_ATTR,
                     OWNER_TYPE,
                     SUPPLIER_ID,
                     ORG_TYPE,
                     ORIGINAL_NUM,
                     LDSTORE_NUM,
                     REFUND_NUM,
                     ASIGIN_NUM,
                     ASIGOUT_NUM,
                     SALE_NUM,
                     SALEBACK_NUM,
                     ADJUSTIN_NUM,
                     ADJUSTOUT_NUM,
                     EXCHGIN_NUM,
                     EXCHGOUT_NUM,
                     STOCK_NUM,
                     CREATE_DATE)
                  VALUES
                    (p_region,
                     p_orgId,
                     p_resKindId,
                     p_statDay,
                     p_statType,
                     p_brandId,
                     p_resTypeId,
                     p_specialAttr,
                     p_ownType,
                     p_supplierId,
                     p_ownType,
                     v_originalNum,
                     v_ldstoreNum,
                     v_refundNum,
                     v_asiginNum,
                     v_asigoutNum,
                     v_saleNum,
                     v_salebackNum,
                     v_adjustinNum,
                     v_adjustoutNum,
                     v_exchginNum,
                     v_exchgoutNum,
                     v_stockNum,
                     sysdate);
           dbms_output.put_line('-->当满足条件时，表 PSI_STAT_DAYPSI 数据插入成功');
                end;
              end if;
            end;
          end loop;
          commit; --处理完一个单位的数据后，提交
        end;
      end loop;
    end;
  end loop;
  commit;
end;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_DAYPSI_BAK"
 IS
  p_region       number(5);
  p_orgId        varchar2(32);
  p_orgType      varchar2(32);
  p_resKindId    varchar2(32) := 'rsclM';
  p_resTypeId    varchar2(32);
  p_brandId      varchar2(32);
  p_specialAttr  varchar2(32);
  p_ownType      varchar2(32);
  p_supplierId   varchar2(32);
  p_statDay      varchar2(32) := to_char(to_date('20301019','yyyy-mm-dd') - 1, 'YYYYMMDD');
  p_statType     varchar2(32) := 'COMMON';
  v_originalNum  number(10);
  v_ldstoreNum   number(10);
  v_refundNum    number(10);
  v_asiginNum    number(10);
  v_asigoutNum   number(10);
  v_saleNum      number(10);
  v_salebackNum  number(10);
  v_adjustinNum  number(10);
  v_adjustoutNum number(10);
  v_exchginNum   number(10);
  v_exchgoutNum  number(10);
  v_stockNum     number(10);
begin
  --首先删除昨天的数据
  delete from PSI_STAT_DAYPSI t
   where t.stat_day = p_statDay
     and t.res_kind_id = p_resKindId
     and t.stat_type = p_statType;
  --循环处理各个地市的数据
  for regionList in (select region from region_list) loop
    begin
      p_region := regionList.region;
      DBMS_OUTPUT.put_line('-->regionList循环：p_region='||p_region || ' p_resKindId=' || p_resKindId || ' p_statDay=' || p_statDay);
      --循环处理各个单位的数据
      for orgList in (select distinct (t.org_id) orgId
                        from PSI_BUSI_STOCK_LOG_HIS t
                       where t.region = p_region
                         and t.res_kind_id = p_resKindId
                         and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                         and t.create_date <
                             to_date(p_statDay, 'YYYYMMDD') + 1) loop
        begin
          p_orgId := orgList.orgid;
          select t.ORGTYPE
            into p_orgType
            from organization t
           where t.ORGID = p_orgId;
          DBMS_OUTPUT.put_line('-->orgList循环：p_orgId='||p_orgId || ' p_orgType=' || p_orgType);
          --循环处理某一个单位下的数据
          for cycleList in (select t.res_type_id  resTypeId,
                                   t.special_attr specialAttr,
                                   t.owner_type   ownerType,
                                   t.supplier_id  supplierId
                              from PSI_BUSI_STOCK_LOG_HIS t
                             where t.region = p_region
                               and t.org_id = p_orgId
                               and t.res_kind_id = p_resKindId
                               and t.create_date >=
                                   to_date(p_statDay, 'YYYYMMDD')
                               and t.create_date <
                                   to_date(p_statDay, 'YYYYMMDD') + 1
                             group by t.res_type_id,
                                      t.special_attr,
                                      t.owner_type,
                                      t.supplier_id) loop
            begin
              p_resTypeId := cycleList.resTypeId;
              select t.brand_id
                into p_brandId
                from im_res_type t
               where t.res_type_id = p_resTypeId;
              p_specialAttr := cycleList.specialAttr;
              p_ownType     := cycleList.ownerType;
              p_supplierId  := cycleList.supplierId;
              DBMS_OUTPUT.put_line('-->cycleList循环：一个循环');
              DBMS_OUTPUT.put_line('-->cycleList循环：p_resTypeId=' || p_resTypeId ||
                                   '  p_brandId=' || p_brandId ||
                                   '  p_specialAttr=' || p_specialAttr ||
                                   '  p_ownType=' || p_ownType ||
                                   '  p_supplierId=' || p_supplierId);
              --入库数量：入库数量为业务类型为入库，但是子业务类型不为盘盈（入库）的数据
              select nvl(sum(t.amount), 0)
                into v_ldstoreNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType                      -- owner_type有可能是null 20140122
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'LDSTORE'
                 and t.sub_busi_type != 'panying';
         dbms_output.put_line('入库数量：v_ldstoreNum='||v_ldstoreNum);
              --退库数量：退库数量为业务类型为退库，但是子业务类型不为盘亏（遗失退库）或盘亏（报废）的数据。
              select nvl(sum(t.amount), 0)
                into v_refundNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'WITHDRAW'
                 and t.sub_busi_type != 'pankui';
         dbms_output.put_line('退库数量：v_refundNum='||v_refundNum);
              --调入数量：调入数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_asiginNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'ASIGOUT'
                 and t.sub_busi_type != 'panying1';
         dbms_output.put_line('调入数量：v_asiginNum='||v_asiginNum);
              --调出数量：调出数量为业务类型为调拨，但是子业务类型不为：盘盈（调拨），出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_asigoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'ASIGOUT'
                 and t.sub_busi_type != 'pankui1';
         dbms_output.put_line('调出数量：v_asigoutNum='||v_asigoutNum);
              --销售数量：销售数量为业务类型为销售的数据。
              select nvl(sum(t.amount), 0)
                into v_saleNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'SALE';
         dbms_output.put_line('销售数量：v_saleNum='||v_saleNum);
              --销售回退数量：销售回退数量为业务类型为销售回退的数据。
              select nvl(sum(t.amount), 0)
                into v_salebackNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'SALEBACK';
         dbms_output.put_line('销售回退数量：v_salebackNum='||v_salebackNum);
              --调整入库数量：调整入库数量为业务类型为入库，子业务类型为盘盈（入库）的数据 或 业务类型为调拨，子业务类型为盘盈（调拨）出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_adjustinNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type in ('LDSTORE', 'ASIGOUT')
                 and t.sub_busi_type in ('panying', 'panying1');
         dbms_output.put_line('调整入库数量：v_adjustinNum='||v_adjustinNum);
              --调整出库数量：调整出库数量为业务类型为退库，子业务类型为盘亏（遗失退库）或盘亏（报废）的数据 或业务类型为调拨，子业务类型为盘盈（调拨）出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_adjustoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type in ('WITHDRAW', 'ASIGOUT')
                 and t.sub_busi_type in ('pankui', 'pankui1');
        dbms_output.put_line('调整出库数量：v_adjustoutNum='||v_adjustoutNum);
              --换货入库数量：换货入库数量为业务类型为换货，出入库标志为入库的数据。
              select nvl(sum(t.amount), 0)
                into v_exchginNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'IN'
                 and t.busi_type = 'EXCHGIN';
        dbms_output.put_line('换货入库数量：v_exchginNum='||v_exchginNum);
              --换货出库数量：换货出库数量为业务类型为换货，出入库标志为出库的数据。
              select nvl(sum(t.amount), 0)
                into v_exchgoutNum
                from PSI_BUSI_STOCK_LOG_HIS t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId
                 and t.create_date >= to_date(p_statDay, 'YYYYMMDD')
                 and t.create_date < to_date(p_statDay, 'YYYYMMDD') + 1
                 and t.inout_flag = 'OUT'
                 and t.busi_type = 'EXCHGOUT';
        dbms_output.put_line('换货出库数量：v_exchgoutNum='||v_exchgoutNum);
              --前一天数量
              select nvl(sum(t.stock_num), 0)
                into v_originalNum
                from PSI_STAT_DAYPSI t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.stat_day = to_char(sysdate - 2, 'YYYYMMDD')
                 and t.stat_type = p_statType
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
        dbms_output.put_line('前一天数量：v_originalNum='||v_originalNum);
              --最新库存量
              select nvl(sum(t.stock_num), 0)
                into v_stockNum
                from PSI_STK_REALTIME_MSTOCK t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.res_type_id = p_resTypeId
                 and t.inv_status = 'INSTORE'
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and (t.owner_type = p_ownType or
                     (p_ownType is null and t.owner_type is null))
                 --and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
         dbms_output.put_line('最新库存量：v_stockNum='||v_stockNum);
              if (v_ldstoreNum != 0 or v_refundNum != 0 or v_asiginNum != 0 or
                 v_asigoutNum != 0 or v_adjustinNum != 0 or
                 v_adjustoutNum != 0 or v_exchginNum != 0 or
                 v_exchgoutNum != 0) then
                begin
                  --数据插入
                  INSERT INTO PSI_STAT_DAYPSI
                    (REGION,
                     ORG_ID,
                     RES_KIND_ID,
                     STAT_DAY,
                     STAT_TYPE,
                     BRAND_ID,
                     RES_TYPE_ID,
                     SPECIAL_ATTR,
                     OWNER_TYPE,
                     SUPPLIER_ID,
                     ORG_TYPE,
                     ORIGINAL_NUM,
                     LDSTORE_NUM,
                     REFUND_NUM,
                     ASIGIN_NUM,
                     ASIGOUT_NUM,
                     SALE_NUM,
                     SALEBACK_NUM,
                     ADJUSTIN_NUM,
                     ADJUSTOUT_NUM,
                     EXCHGIN_NUM,
                     EXCHGOUT_NUM,
                     STOCK_NUM,
                     CREATE_DATE)
                  VALUES
                    (p_region,
                     p_orgId,
                     p_resKindId,
                     p_statDay,
                     p_statType,
                     p_brandId,
                     p_resTypeId,
                     p_specialAttr,
                     p_ownType,
                     p_supplierId,
                     p_ownType,
                     v_originalNum,
                     v_ldstoreNum,
                     v_refundNum,
                     v_asiginNum,
                     v_asigoutNum,
                     v_saleNum,
                     v_salebackNum,
                     v_adjustinNum,
                     v_adjustoutNum,
                     v_exchginNum,
                     v_exchgoutNum,
                     v_stockNum,
                     sysdate);
                end;
              end if;
            end;
          end loop;
          commit; --处理完一个单位的数据后，提交
        end;
      end loop;
    end;
  end loop;
  commit;
end;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_MONTHPSI"
 IS
  p_region       number(5);
  p_orgId        varchar2(32);
  p_orgType      varchar2(32);
  p_resKindId    varchar2(32) := 'rsclM';
  p_resTypeId    varchar2(32);
  p_brandId      varchar2(32);
  p_specialAttr  varchar2(32);
  p_ownType      varchar2(32);
  p_supplierId   varchar2(32);
  p_statMonth    varchar2(32) := to_char(add_months(to_date('20301019','yyyy-mm-dd'), -1), 'yyyymm');
  p_statType     varchar2(32) := 'COMMON';
  v_originalNum  number(10);
  v_ldstoreNum   number(10);
  v_refundNum    number(10);
  v_asiginNum    number(10);
  v_asigoutNum   number(10);
  v_saleNum      number(10);
  v_salebackNum  number(10);
  v_adjustinNum  number(10);
  v_adjustoutNum number(10);
  v_exchginNum   number(10);
  v_exchgoutNum  number(10);
  v_stockNum     number(10);
  p_earlyStatDay  varchar2(32);
  p_latestStatDay varchar2(32);
begin
  --首先删除上月的数据
  delete from PSI_STAT_MONTHPSI t
   where t.stat_month = p_statMonth
     and t.res_kind_id = p_resKindId
     and t.stat_type = p_statType;
  --循环处理各个地市的数据
  for regionList in (select region from region_list) loop
    begin
      p_region := regionList.region;
      DBMS_OUTPUT.put_line('-->regionList循环：p_region='||p_region || ' p_resKindId=' || p_resKindId || ' p_statMonth=' || p_statMonth);
      --循环处理各个单位的数据
      for orgList in (select distinct (t.org_id) orgId
                        from PSI_STAT_DAYPSI t
                       where t.region = p_region
                         and t.res_kind_id = p_resKindId
                         and t.stat_type = p_statType
                         and t.stat_day >= p_statMonth
                         and t.stat_day <= p_statMonth || '99') loop
        begin
          p_orgId := orgList.orgid;
          select t.ORGTYPE
            into p_orgType
            from organization t
           where t.ORGID = p_orgId;
          DBMS_OUTPUT.put_line('-->orgList循环：p_orgId='||p_orgId || ' p_orgType=' || p_orgType);
          --循环处理某一个单位下的数据
          for cycleList in (select t.res_type_id resTypeId,
                                   t.special_attr specialAttr,
                                   t.owner_type ownerType,
                                   t.supplier_id supplierId,
                                   sum(t.ldstore_num) ldstoreNum,
                                   sum(t.refund_num) refundNum,
                                   sum(t.asigin_num) asigninNum,
                                   sum(t.asigout_num) asignoutNum,
                                   sum(t.sale_num) saleNum,
                                   sum(t.saleback_num) salebackNum,
                                   sum(t.adjustin_num) adjustinNum,
                                   sum(t.adjustout_num) adjustoutNum,
                                   sum(t.exchgin_num) exchginNum,
                                   sum(t.exchgout_num) exchgoutNum
                              from PSI_STAT_DAYPSI t
                             where t.region = p_region
                               and t.org_id = p_orgId
                               and t.res_kind_id = p_resKindId
                               and t.stat_type = p_statType
                               and t.stat_day >= p_statMonth
                               and t.stat_day <= p_statMonth || '99'
                             group by t.res_type_id,
                                      t.special_attr,
                                      t.owner_type,
                                      t.supplier_id) loop
            begin
              p_resTypeId := cycleList.resTypeId;
              select t.brand_id
                into p_brandId
                from im_res_type t
               where t.res_type_id = p_resTypeId;
              p_specialAttr := cycleList.specialAttr;
              p_ownType     := cycleList.ownerType;
              p_supplierId  := cycleList.supplierId;
              DBMS_OUTPUT.put_line('-->cycleList循环：一个循环');
              DBMS_OUTPUT.put_line('--->cycleList循环：p_resTypeId=' || p_resTypeId ||
                                   '  p_brandId=' || p_brandId ||
                                   '  p_specialAttr=' || p_specialAttr ||
                                   '  p_ownType=' || p_ownType ||
                                   '  p_supplierId=' || p_supplierId);

              v_ldstoreNum   := cycleList.ldstoreNum;
              v_refundNum    := cycleList.refundNum;
              v_asiginNum    := cycleList.asigninNum;
              v_asigoutNum   := cycleList.asignoutNum;
              v_saleNum      := cycleList.saleNum;
              v_salebackNum  := cycleList.salebackNum;
              v_adjustinNum  := cycleList.adjustinNum;
              v_adjustoutNum := cycleList.adjustoutNum;
              v_exchginNum   := cycleList.exchginNum;
              v_exchgoutNum  := cycleList.exchgoutNum;
        dbms_output.put_line('-->cycleList循环前：v_ldstoreNum='||v_ldstoreNum||
                            ' v_refundNum= '||v_refundNum||
                            ' v_asiginNum= '||v_asiginNum||
                            ' v_asigoutNum= '||v_asigoutNum||
                            ' v_saleNum= '||v_saleNum||
                            ' v_salebackNum= '||v_salebackNum||
                            ' v_adjustinNum= '||v_adjustinNum||
                            ' v_adjustoutNum= '||v_adjustoutNum||
                            ' v_exchginNum= '||v_exchginNum||
                            ' v_exchgoutNum= '||v_exchgoutNum);
              --该月最早的一天和最晚的一条
              select min(t.stat_day), max(t.stat_day)
                into p_earlyStatDay, p_latestStatDay
                from PSI_STAT_DAYPSI t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.stat_day >= p_statMonth
                 and t.stat_day <= p_statMonth || '99'
                 and t.stat_type = p_statType
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
              --期初数量
              select nvl(sum(t.original_num), 0)
                into v_originalNum
                from PSI_STAT_DAYPSI t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.stat_day = p_earlyStatDay
                 and t.stat_type = p_statType
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
              --期末数量
              select nvl(sum(t.stock_num), 0)
                into v_stockNum
                from PSI_STAT_DAYPSI t
               where t.region = p_region
                 and t.org_id = p_orgId
                 and t.res_kind_id = p_resKindId
                 and t.stat_day = p_latestStatDay
                 and t.stat_type = p_statType
                 and t.res_type_id = p_resTypeId
                 and (t.special_attr = p_specialAttr or
                     (p_specialAttr is null and t.special_attr is null))
                 and t.owner_type = p_ownType
                 and t.supplier_id = p_supplierId;
              DBMS_OUTPUT.put_line('-->cycleList循环后：v_ldstoreNum=' || v_ldstoreNum ||
                                   '  v_refundNum=' || v_refundNum ||
                                   '  v_asiginNum=' || v_asiginNum ||
                                   '  v_asigoutNum=' || v_asigoutNum ||
                                   '  v_adjustinNum=' || v_adjustinNum ||
                                   '  v_adjustoutNum=' || v_adjustoutNum ||
                                   '  v_exchginNum=' || v_exchginNum ||
                                   '  v_exchgoutNum=' || v_exchgoutNum);
              if (v_ldstoreNum != 0 or v_refundNum != 0 or v_asiginNum != 0 or
                 v_asigoutNum != 0 or v_adjustinNum != 0 or
                 v_adjustoutNum != 0 or v_exchginNum != 0 or
                 v_exchgoutNum != 0) then
                begin
                  --数据插入
                  INSERT INTO PSI_STAT_MONTHPSI
                    (REGION,
                     ORG_ID,
                     RES_KIND_ID,
                     STAT_MONTH,
                     STAT_TYPE,
                     BRAND_ID,
                     RES_TYPE_ID,
                     SPECIAL_ATTR,
                     OWNER_TYPE,
                     SUPPLIER_ID,
                     ORG_TYPE,
                     ORIGINAL_NUM,
                     LDSTORE_NUM,
                     REFUND_NUM,
                     ASIGIN_NUM,
                     ASIGOUT_NUM,
                     SALE_NUM,
                     SALEBACK_NUM,
                     ADJUSTIN_NUM,
                     ADJUSTOUT_NUM,
                     EXCHGIN_NUM,
                     EXCHGOUT_NUM,
                     STOCK_NUM,
                     CREATE_DATE)
                  VALUES
                    (p_region,
                     p_orgId,
                     p_resKindId,
                     p_statMonth,
                     p_statType,
                     p_brandId,
                     p_resTypeId,
                     p_specialAttr,
                     p_ownType,
                     p_supplierId,
                     p_ownType,
                     v_originalNum,
                     v_ldstoreNum,
                     v_refundNum,
                     v_asiginNum,
                     v_asigoutNum,
                     v_saleNum,
                     v_salebackNum,
                     v_adjustinNum,
                     v_adjustoutNum,
                     v_exchginNum,
                     v_exchgoutNum,
                     v_stockNum,
                     sysdate);
           dbms_output.put_line('-->当满足条件时表 PSI_STAT_MONTHPSI 数据插入成功');
                end;
              end if;
            end;
          end loop;
          commit; --处理完一个单位的数据后，提交
        end;
      end loop;
    end;
  end loop;
  commit;
end;
/

CREATE OR REPLACE PROCEDURE "LOC_1126_SELLRANKINGLIST" (projectid in VARCHAR2) is
  v_count number;
begin
  if projectid is null then
    dbms_output.put_line('SD_SUP_MET_20161205');
    return;
  end if;
  select count(*)
    into v_count
    from psi_flow_apply
   where project_id = projectid;
  if v_count = 0 then
    dbms_output.put_line('未找到本次活动的PROJECT_ID:' || projectid);
    return;
  end if;
  --地区订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_CityRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_CityRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, region, attr1, static_amt)
    select ranking_type,
                    region,
                    regionname,
                    sum(static_amt) static_amt
               from (select 'GHH_CityRankList' ranking_type,
                            b.region region,
                            b.regionname regionname,
                            nvl(a.sum_apply_num, 0) static_amt
                       from psi_flow_apply a, region_list b
                      where a.project_id = projectid
                        and a.region = b.region
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
                     union all
                     select 'GHH_CityRankList' ranking_type,
                            m.region region,
                            m.regionname regionname,
                            nvl(SUM_ORDER_NUM, 0) static_amt
                       from PSI_INFO_DEPOSIT t, region_list m
                      where t.prj_id = projectid
                        and t.region = m.region)
              group by (ranking_type, region, regionname)
              order by static_amt desc;
  commit;
  --机型订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_MobRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_MobRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, res_type_id, attr1, static_amt)
    (select *
       from (select ranking_type,
                    res_type_id,
                    res_type_name,
                    sum(static_amt) static_amt
               from (select 'GHH_MobRankList' ranking_type,
                            c.res_type_id res_type_id,
                            c.res_type_name res_type_name,
                            nvl(b.apply_num, 0) static_amt
                       from psi_flow_apply       a,
                            psi_flow_apply_batch b,
                            im_res_type          c
                      where a.project_id = projectid
                        and a.flow_id = b.flow_id
                        and c.res_type_id = b.res_type_id
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
                     union all
                     select 'GHH_MobRankList' ranking_type,
                            m.res_type_id res_type_id,
                            (select a.res_type_name from im_res_type a where a.res_type_id=
                                    m.res_type_id) res_type_name,
                            nvl(m.order_num, 0) static_amt
                       from psi_info_deposit t, psi_info_deposit_restype m
                      where t.prj_id = projectid
                        and t.oid = m.deposit_oid)
              group by (ranking_type, res_type_id, res_type_name)
              order by static_amt desc));
  commit;
  --渠道订货排行榜
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_ChanRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_ChanRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1, REGION, static_amt)
   select ranking_type,
                    orgid,
                    orgname,
                    region,
                    sum(static_amt) static_amt
               from (select 'GHH_ChanRankList' ranking_type,
                            b.orgid orgid,
                            substr(b.orgname, 1, 128) orgname,
                            a.region region,
                            nvl(a.sum_apply_num, 0) static_amt
                       from psi_flow_apply a, organization b
                      where a.project_id = projectid
                        and a.org_id = b.orgid
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
                     union all
                     select 'GHH_ChanRankList' ranking_type,
                            t.agent_id orgid,
                            substr(m.orgname, 1, 128) orgname,
                            t.region region,
                            nvl(SUM_ORDER_NUM, 0) static_amt
                       from PSI_INFO_DEPOSIT t, organization m
                      where t.prj_id = projectid
                        and t.agent_id = m.orgid)
              group by (ranking_type, orgid, orgname, region)
              order by static_amt desc;
  commit;
  --供货商订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_SupplierRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_SupplierRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1, static_amt)
    select ranking_type, org_id, attr1, sum(static_amt) static_amt
               from (select 'GHH_SupplierRankList' ranking_type,
                            c.orgid org_id,
                            c.orgname attr1,
                            nvl(b.apply_num, 0) static_amt
                       from psi_flow_apply       a,
                            psi_flow_apply_batch b,
                            organization     c
                      where a.project_id = projectid
                        and a.flow_id = b.flow_id
                        and c.orgid = b.supplier_id
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
                     union all
                     select 'GHH_SupplierRankList' ranking_type,
                            m.partner_id org_id,
                            m.partner_name attr1,
                            nvl(t.sum_order_num, 0) static_amt
                       from PSI_INFO_DEPOSIT t, psi_info_partner m
                      where t.prj_id = projectid
                        and m.partner_id = t.supplier_id)
              group by (ranking_type, org_id, attr1)
              order by static_amt desc;
  commit;
  --新加-------------------------------------
  --总订货量、总订货金额、总订单量
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_TotalRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_TotalRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, static_amt, static_amt1, static_amt2)
    select 'GHH_TotalRankList', sum(a1), sum(a2),sum(a3)
      from (select sum(a.SUM_APPLY_NUM) a1,
                   sum(round((a.sum_price / 100), 0)) a2,
                   count(flow_id) a3
              from psi_flow_apply a
             where a.project_id = projectid
               and a.apply_type in
                   ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
               and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
            union all
            select sum(b.SUM_ORDER_NUM) a1,
                   sum(round((b.DEPOSIT / 100), 0)) a2,
                   0 a3
              from PSI_INFO_DEPOSIT b
             where b.prj_id = projectid);

             update PSI_INFO_MOB_RANKINGLIST t set t.static_amt2=t.static_amt2+13000
                    where ranking_type = 'GHH_TotalRankList';
  commit;
  --各地市参与渠道数
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_CityChanRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_CityChanRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, attr1, static_amt)
    select ranking_type, regionname, count(distinct crtid)
      from (select 'GHH_CityChanRankList' ranking_type,
                   b.regionname regionname,
                   a.create_org_id crtid
              from psi_flow_apply a, region_list b
             where a.project_id = projectid
               and a.region = b.region
               and a.apply_type in
                   ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
               and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
            union all
            select 'GHH_CityChanRankList' ranking_type,
                   b.regionname regionname,
                   c.agent_Id crtid
              from PSI_INFO_DEPOSIT c, Region_List b
             where c.prj_id = projectid
               and c.region = b.region)
     group by (ranking_type, regionname);

update PSI_INFO_MOB_RANKINGLIST t set t.static_amt=t.static_amt+round(11000*t.static_amt/(select
       sum(a.static_amt) from PSI_INFO_MOB_RANKINGLIST a where ranking_type = 'GHH_CityChanRankList'),0)
       where ranking_type = 'GHH_CityChanRankList';
  --group by (c.res_type_id, c.res_type_name));
  commit;
  --各品牌订货排名
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_BrandRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'GHH_BrandRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type,brand_id,attr1,static_amt)
 select ranking_type,
                 brand_id,
                 brand_name,
                 sum(static_amt) static_amt
            from (select 'GHH_BrandRankList' ranking_type,
                         b.brand_id brand_id,
                         (select c.brand_name from im.psi_cfg_resbrand c where c.brand_id= b.brand_id) brand_name,
                         nvl(b.apply_num,0) as static_amt
                    from psi_flow_apply       a,
                         psi_flow_apply_batch b
                   where a.project_id = projectid
                     and a.FLOW_ID = b.FLOW_ID
                     and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                          'CATA_MOBAPPLY_AGENTPRESELL')
                     and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
                  union all
                  select 'GHH_BrandRankList' ranking_type,
                         m.brand_id brand_id,
                         m.brand_name brand_name,
                         m.ORDER_NUM static_amt
                    from psi_info_deposit t, psi_info_deposit_restype m
                   where t.prj_id = projectid
                     and t.oid = m.deposit_oid)
           group by (ranking_type, brand_id, brand_name)
           order by static_amt desc;
   commit;
end Loc_1126_SellRankingList;
/


 CREATE OR REPLACE PROCEDURE "LOC_CROWDFUNDING_RANKING" (projectid in VARCHAR2) is
  v_count number;
begin

  if projectid is null then
    dbms_output.put_line('请传入本次活动的PROJECT_ID');
    return;
  end if;

  select count(*)
    into v_count
    from psi_flow_apply
   where project_id = projectid;

  if v_count = 0 then
    dbms_output.put_line('未找到本次活动的PROJECT_ID:' || projectid);
    return;
  end if;

  --地区订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_CityRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'CRW_CityRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, region, attr1, static_amt,static_amt1)
    select d.ranking_type,
           d.region,
           (select r.regionname from REGION_LIST R where R.REGION = d.region),
           sum(nvl(d.static_amt,0)) static_amt,
           sum(nvl(d.static_amt1,0)) static_amt1
      from (select 'CRW_CityRankList' ranking_type,
                   a.region region,
                   nvl(a.sum_apply_num, 0) static_amt,
                   0 static_amt1
              from psi_flow_apply a
             where a.project_id = projectid
               and a.apply_type in
                   ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
               and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
            UNION ALL
            select 'CRW_CityRankList' ranking_type,
                   a.region region,
                   0 static_amt,
                   nvl(a.sum_apply_num, 0) static_amt1
              from psi_flow_apply a
             where a.project_id = projectid
               and a.apply_type = 'CATA_MOBAPPLY_PRECROWDFUNDING'
               and a.flow_status = 'FINISHED') d
    group by ranking_type, region
    order by static_amt1 desc;
  commit;

  --机型订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_MobRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'CRW_MobRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, res_type_id, attr1, static_amt,static_amt1)
    select d.ranking_type,
           d.res_type_id,
           (select i.res_type_name from im_res_type i where i.res_type_id=d.res_type_id),
           sum(nvl(d.static_amt,0)) static_amt,
           sum(nvl(d.static_amt1,0)) static_amt1
    from (select 'CRW_MobRankList' ranking_type,
                 b.res_type_id res_type_id,
                 nvl(b.apply_num, 0) static_amt,
                 0 static_amt1
            from psi_flow_apply a, psi_flow_apply_batch b
           where a.project_id = projectid
             and a.flow_id = b.flow_id
             and a.apply_type in
                 ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
             and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
          union all
           select 'CRW_MobRankList' ranking_type,
                  b.res_type_id res_type_id,
                  0 static_amt,
                  nvl(b.apply_num, 0) static_amt1
            from psi_flow_apply a, psi_flow_apply_batch b
           where a.project_id = projectid
             and a.flow_id = b.flow_id
             and a.apply_type = 'CATA_MOBAPPLY_PRECROWDFUNDING'
             and a.flow_status = 'FINISHED') d
  group by ranking_type, res_type_id
  order by static_amt1 desc;
  commit;

  --渠道订货排行榜
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_ChanRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'CRW_ChanRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1,REGION, static_amt,static_amt1)

       select d.ranking_type,
              d.orgid,
              (select substr(o.orgname, 1, 128) from organization o where o.ORGID=d.orgid),
              d.region,
              sum(nvl(d.static_amt,0)) static_amt,
              sum(nvl(d.static_amt1,0)) static_amt1
          from (select 'CRW_ChanRankList' ranking_type,
                    a.org_id orgid,
                    a.region region,
                    nvl(a.sum_apply_num, 0) static_amt,
                    0 static_amt1
               from psi_flow_apply a
              where a.project_id = projectid
                and a.apply_type in('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
                and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
               union all
               select 'CRW_ChanRankList' ranking_type,
                      a.org_id orgid,
                      a.region,
                      0 static_amt,
                      nvl(a.sum_apply_num, 0) static_amt1
                 from psi_flow_apply a
                where a.project_id = projectid
                  and a.apply_type = 'CATA_MOBAPPLY_PRECROWDFUNDING'
                  and a.flow_status = 'FINISHED')d
                 group by ranking_type,d.orgid,d.region
                 order by static_amt1 desc;
  commit;

  --供货商订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='GHH_SupplierRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'CRW_SupplierRankList';
  --插入新数据
    insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1, static_amt,static_amt1)
    select d.ranking_type,
           d.supplier_id,
           (select p.partner_name from psi_info_partner p where p.partner_id=d.supplier_id ),
           sum(nvl(d.static_amt,0)) static_amt,
           sum(nvl(d.static_amt1,0)) static_amt1
      from (select 'CRW_SupplierRankList' ranking_type,
                    b.supplier_id supplier_id,
                    nvl(b.apply_num, 0) static_amt,
                    0 static_amt1
               from psi_flow_apply a, psi_flow_apply_batch b
              where a.project_id = projectid
                and a.flow_id = b.flow_id
                and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
                and a.flow_status in ('UNCONSIGNMENT', 'FINISHED')
             union all
             select 'CRW_SupplierRankList' ranking_type,
                    b.supplier_id supplier_id,
                    0 static_amt,
                    nvl(b.apply_num, 0) static_amt1
               from psi_flow_apply a, psi_flow_apply_batch b
              where a.project_id =projectid
                and a.flow_id = b.flow_id
                and a.apply_type = 'CATA_MOBAPPLY_PRECROWDFUNDING'
                and a.flow_status = 'FINISHED')d
     group by ranking_type, d.supplier_id
     order by static_amt1 desc;
 commit;
end Loc_Crowdfunding_Ranking;
/

CREATE OR REPLACE PROCEDURE "LOC_SPIKE_RANKING" is
  v_count number;
begin
  select count(*)
    into v_count
    from psi_flow_apply
   where project_id in(select dict_id from psi_dict_item where group_id='Spike_TIMESLOIT' and status=1);
   dbms_output.put_line('-->v_count='||v_count);
  if v_count = 0 then
    dbms_output.put_line('未找到字典组SaleGoodsSpike的定义');
    return;
  end if;
  --地区订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='SPIKE_CityRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'SPIKE_CityRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, region, attr1, static_amt)
    (select *
       from (select ranking_type,
                    region,
                    regionname,
                    sum(static_amt) static_amt
               from (select 'SPIKE_CityRankList' ranking_type,
                            b.region region,
                            b.regionname regionname,
                            nvl(a.sum_apply_num, 0) static_amt
                       from psi_flow_apply a, region_list b
                      where a.project_id in(select dict_id from psi_dict_item where group_id='Spike_TIMESLOIT' and status=1)
                        and a.region = b.region
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED'))
              group by (ranking_type, region, regionname)
              order by static_amt desc)
      );
  commit;
  --机型订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='SPIKE_MobRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'SPIKE_MobRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, res_type_id, attr1, static_amt)
    (select *
       from (select ranking_type,
                    res_type_id,
                    res_type_name,
                    sum(static_amt) static_amt
               from (select 'SPIKE_MobRankList' ranking_type,
                            c.res_type_id res_type_id,
                            c.res_type_name res_type_name,
                            nvl(b.apply_num, 0) static_amt
                       from psi_flow_apply       a,
                            psi_flow_apply_batch b,
                            im_res_type          c
                      where a.project_id in(select dict_id from psi_dict_item where group_id='Spike_TIMESLOIT' and status=1)
                        and a.flow_id = b.flow_id
                        and c.res_type_id = b.res_type_id
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED'))
              group by (ranking_type, res_type_id, res_type_name)
              order by static_amt desc));
  commit;
  --渠道订货排行榜
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='SPIKE_ChanRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'SPIKE_ChanRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1, REGION, static_amt)
    (select *
       from (select ranking_type,
                    orgid,
                    orgname,
                    region,
                    sum(static_amt) static_amt
               from (select 'SPIKE_ChanRankList' ranking_type,
                            b.orgid orgid,
                            substr(b.orgname, 1, 128) orgname,
                            a.region region,
                            nvl(a.sum_apply_num, 0) static_amt
                       from psi_flow_apply a, organization b
                      where a.project_id in(select dict_id from psi_dict_item where group_id='Spike_TIMESLOIT' and status=1)
                        and a.org_id = b.orgid
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED'))
              group by (ranking_type, orgid, orgname, region)
              order by static_amt desc)
     );
  commit;
  --供货商订货量排行
  --先删除PSI_INFO_MOB_RANKINGLIST表中ranking_type='SPIKE_SupplierRankList'的记录
  delete from PSI_INFO_MOB_RANKINGLIST
   where ranking_type = 'SPIKE_SupplierRankList';
  --插入新数据
  insert into PSI_INFO_MOB_RANKINGLIST
    (ranking_type, org_id, attr1, static_amt)
    (select *
       from (select ranking_type, org_id, attr1, sum(static_amt) static_amt
               from (select 'SPIKE_SupplierRankList' ranking_type,
                            c.orgid org_id,
                            c.orgname attr1,
                            nvl(b.apply_num, 0) static_amt
                       from psi_flow_apply       a,
                            psi_flow_apply_batch b,
                            organization     c
                      where a.project_id in(select dict_id from psi_dict_item where group_id='Spike_TIMESLOIT' and status=1)
                        and a.flow_id = b.flow_id
                        and c.orgid = b.supplier_id
                        and a.apply_type in ('CATA_MOBAPPLY_AGENTSELL',
                             'CATA_MOBAPPLY_AGENTPRESELL')
                        and a.flow_status in ('UNCONSIGNMENT', 'FINISHED'))
              group by (ranking_type, org_id, attr1)
              order by static_amt desc)
     );
  commit;
  dbms_output.put_line('-->程序结束');
end Loc_Spike_Ranking;
/

CREATE OR REPLACE PROCEDURE "P_DDS_GENERATE_SEQ" (schema_id_input in varchar2, seq_name_input in varchar2, seq_mode in varchar2, node_order in varchar2, node_count in varchar2, batch_count in varchar2, returnvalue out varchar2, result_code out varchar2, result_inf out varchar2) as
v_isFound integer;
CURSOR c_seqRecords IS select start_value,min_value,max_value,increment_by, now_value, is_cycle FROM t_dds_sequence t WHERE t.seq_name = seq_name_input and t.schema_id=schema_id_input for update;
CURSOR c_seqCurr IS select start_value,min_value,max_value,increment_by, now_value FROM t_dds_sequence t WHERE t.seq_name = seq_name_input and t.schema_id=schema_id_input;
lastSeqValue NUMBER;
begin
    returnvalue := '0';
    result_code := '0';
    result_inf := 'succ';
    v_isFound := 0;
  dbms_output.put_line('-->程序开始前出参的值分别为：returnvalue='||returnvalue||' result_code='||result_code||' result_inf='||result_inf);
    if to_number(seq_mode) = 2 then --nextvalue
        FOR c_rec IN c_seqRecords LOOP
            if c_rec.now_value is null then
              returnvalue := to_char(c_rec.start_value+to_number(node_order));
        dbms_output.put_line('-->seq_mode=2时：returnvalue1=' || returnvalue);
            elsif c_rec.now_value < c_rec.start_value then
              returnvalue := to_char(c_rec.start_value+to_number(node_order));
        dbms_output.put_line('-->seq_mode=2时：returnvalue2=' || returnvalue);
            else
              returnvalue := to_char(c_rec.now_value+(to_number(node_count)*to_number(c_rec.increment_by)));
        dbms_output.put_line('-->seq_mode=2时：returnvalue3=' || returnvalue);
            end if;

            lastSeqValue := to_number(returnvalue)+ to_number(node_count)*to_number(c_rec.increment_by)*(to_number(batch_count)-1);
      dbms_output.put_line('-->lastSeqValue=' || lastSeqValue);
            IF c_rec.is_cycle = 1 AND lastSeqValue > c_rec.max_value THEN
                returnvalue := to_char(c_rec.start_value+to_number(node_order));
                lastSeqValue := to_number(returnvalue)+ to_number(node_count)*to_number(c_rec.increment_by)*(to_number(batch_count)-1);
        dbms_output.put_line('-->returnvalue=' || returnvalue);
        dbms_output.put_line('-->lastSeqValue=' || lastSeqValue);
      END IF;
      dbms_output.put_line('-->max_value=' || c_rec.max_value);
            IF lastSeqValue > c_rec.max_value THEN
                result_code := '1';
                result_inf := 'sequence exceeded the maximum range';
        dbms_output.put_line('-->result_code=' || result_code);
        dbms_output.put_line('-->result_inf=' || result_inf);
            ELSE
                update t_dds_sequence set now_value=lastSeqValue where CURRENT OF c_seqRecords;
            END IF;
            v_isFound := 1;
      dbms_output.put_line('-->v_isFound=' || v_isFound);
        END LOOP;
        if v_isFound<>1 then
            result_code := '1';
            result_inf := 'sequence is not exist in DB';
      dbms_output.put_line('-->elsif:result_code=' || result_code);
      dbms_output.put_line('-->elsif:result_inf=' || result_inf);
        end if;
        commit;
    elsif to_number(seq_mode) = 1 then --currvalue
        FOR c_rec2 IN c_seqCurr LOOP
            if c_rec2.now_value is null then
              returnvalue := to_char(c_rec2.start_value);
        dbms_output.put_line('-->elsif,now_value为null时:returnvalue=' || returnvalue);
            else
              returnvalue := to_char(c_rec2.now_value);
        dbms_output.put_line('-->else,now_value不为null时:returnvalue=' || returnvalue);
            end if;
            v_isFound := 1;
      dbms_output.put_line('-->v_isFound='||v_isFound);
        END LOOP;
        if v_isFound<>1 then
            result_code := '1';
            result_inf := 'sequence is not exist in DB';
        end if;
    else
        result_code := '1';
        result_inf := 'invalid sequence mode';
    dbms_output.put_line('-->else:result_code=' || result_code);
    dbms_output.put_line('-->else:result_inf=' || result_inf);
    end if;
  dbms_output.put_line('-->程序结束后出参的值分别为：returnvalue='||returnvalue||' result_code='||result_code||' result_inf='||result_inf);
end p_DDS_generate_seq;
/

CREATE OR REPLACE PROCEDURE "LOC_PSI_CFG_SUPPLIER_RES_PRC" (i_region in varchar2) is
  var_count             varchar2(32);
  var_manual_flag       varchar2(32); --人工设置标志，当值为Manual是表示人工设置的水位
  var_supplier_id       varchar2(32); --供货商
  var_res_type_id       varchar2(32); --机型
  var_brand_count       varchar2(32); --当前库存量
  var_agent_order_count varchar2(32); --此供货商的社会渠道的此机型订单量（撤销、已发货状态除外）
  var_self_order_count  varchar2(32); --供货商自有渠道此机型订单（状态为市市场部审批通过、市其他领导审批、省领导审批、供货平台确认、未发货完毕的订单量总和）
  var_elec_order_count  varchar2(32); --获取电子渠道订单量（平台已确认、未发货）
  --1.  从供货关系表psi_cfg_supplier_res中正在生效的供货记录，逐条循环。
  Cursor myCursor is
    select SUPPLIER_ID, RES_TYPE_ID
      from psi_cfg_supplier_res
     where region = i_region
       and eff_date <= sysdate
       and (exp_date is null or exp_date > sysdate)
       and status = 'VALID';
begin
  open myCursor;
  loop
    fetch myCursor
      into var_supplier_id, var_res_type_id; --相当于foreach
    exit when myCursor%notfound; --判断如果没有找到
  dbms_output.put_line('-->1：var_supplier_id='||var_supplier_id||' var_res_type_id='||var_res_type_id);
    begin
      --2.  判断此机型在PSI_CFG_VENDOR_SUPPLYINFO表中是否存在记录（包括供货商），如果不存在或‘手工标志为不是MANUAL’,则继续向下处理。
      select count(*)
        into var_count
        from PSI_CFG_VENDOR_SUPPLYINFO
       where vendor_id = var_supplier_id
         and res_type_id = var_res_type_id;
    dbms_output.put_line('-->2：var_count='||var_count);
      if var_count <= 0 then
        --3.  获取供货商和机型信息，从psi_stk_realtime_mstock中获取此供货商可用的机型的当前库存量。
        select nvl(sum(stock_num), 0)
          into var_brand_count
          from psi_stk_realtime_mstock
         where supplier_id = var_supplier_id
           and RES_TYPE_ID = var_res_type_id
           and inv_status = 'INSTORE'
           and busi_status = 'USABLE';
    dbms_output.put_line('-->3，var_count <= 0时：var_brand_count='||var_brand_count);
        --4.  获取此供货商的社会渠道的此机型订单量（撤销、已发货状态除外）。
        select nvl(sum(m.apply_num), 0)
          into var_agent_order_count
          from psi_flow_apply t, psi_flow_apply_batch m
         where m.Supplier_Id = var_supplier_id
           and m.res_type_id = var_res_type_id
           and t.apply_type in
               ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
              --and t.flow_id = m.flow_id and flow_status not in ('撤销','已发货');
           and t.flow_id = m.flow_id
           and flow_status not in
               (select dictid from dict_item where groupid = 'apply_flow_status4');
        dbms_output.put_line('-->4，var_count <= 0时：var_agent_order_count='||var_agent_order_count);   
        --5.  获取供货商自有渠道此机型订单（状态为市市场部审批通过、市其他领导审批、省领导审批、供货平台确认、未发货完毕的订单量总和）
        select nvl(sum(m.apply_num), 0)
          into var_self_order_count
          from psi_flow_apply t, psi_flow_apply_batch m
         where m.Supplier_Id = var_supplier_id
           and m.res_type_id = var_res_type_id
           and t.apply_type in
               ('CATA_MOBAPPLY_PUBLIC_SELL', 'CATA_MOBAPPLY_GROUP_SELL',
                'CATA_MOBAPPLY_PUBLIC_SELL_A2S',
                'CATA_MOBAPPLY_GROUP_SELL_A2S')
              --and t.flow_id = m.flow_id and flow_status in ('市场部审批通过','市其他领导审批','省领导审批','供货平台确认','未发货完毕');
           and t.flow_id = m.flow_id
           and (flow_status in
               (select dictid from dict_item where groupid = 'apply_flow_status5') or m.apply_num >  m.stockup_num);
      dbms_output.put_line('-->5，var_count <= 0时：var_self_order_count='||var_self_order_count);   
    --6.  获取电子渠道订单量（平台已确认、未发货）
        select nvl(sum(m.apply_num), 0)
          into var_elec_order_count
          from psi_flow_apply t, psi_flow_apply_batch m
         where m.Supplier_Id = var_supplier_id
           and m.res_type_id = var_res_type_id
           and t.apply_type in ('CATA_MOBAPPLY_ECHANNEL_SELL')
              --and t.flow_id = m.flow_id and flow_status in ('平台已确认','未发货');
           and t.flow_id = m.flow_id
           and (flow_status in
               (select dictid from dict_item where groupid = 'apply_flow_status6')  or m.apply_num >  m.stockup_num);
     dbms_output.put_line('-->6，var_count <= 0时：var_elec_order_count='||var_elec_order_count);    
        --7.  当前库存量=供货商实时库存-供货商社会渠道的此机型订单量-供货商自有渠道此机型订单-电子渠道订单量。
        var_brand_count := var_brand_count - var_agent_order_count -
                           var_self_order_count - var_elec_order_count;
     dbms_output.put_line('-->7，var_count <= 0时：var_brand_count='||var_brand_count);    
        --8.  将此库存量更新到PSI_CFG_VENDOR_SUPPLYINFO表中。如果记录不存在则新增记录，并且手工标志为空。
        insert into PSI_CFG_VENDOR_SUPPLYINFO
          (region,
           Oid,
           Vendor_Id,
           Supplier_Type,
           res_type_id,
           Supply_Month,
           Supply_Num,
           Left_Num,
           Create_Date)
        values
          (i_region,
           seq_oid.nextval,
           var_supplier_id,
           'PMPSuppTerm',
           var_res_type_id,
           to_char(sysdate, 'mm'),
           var_brand_count,
           var_brand_count,
           sysdate);
        commit;
      else
        begin
          select nvl(manual_flag, '0')
            into var_manual_flag
            from psi_cfg_vendor_supplyinfo
           where vendor_id = var_supplier_id
             and res_type_id = var_res_type_id;
       dbms_output.put_line('-->8，var_count > 0时：var_manual_flag='||var_manual_flag);     
        exception
          when others then
            dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
                 ' || SQLERRM);
            return;
        end;

        if var_manual_flag <> 'MANUAL' then
          --3.  获取供货商和机型信息，从psi_stk_realtime_mstock中获取此供货商可用的机型的当前库存量。
          select nvl(sum(stock_num), 0)
            into var_brand_count
            from psi_stk_realtime_mstock
           where supplier_id = var_supplier_id
             and RES_TYPE_ID = var_res_type_id
             and inv_status = 'INSTORE'
             and busi_status = 'USABLE';
      dbms_output.put_line('-->当 var_manual_flag !=  MANUAL：var_brand_count='||var_brand_count);    
          --4.  获取此供货商的社会渠道的此机型订单量（撤销、已发货状态除外）。
          select nvl(sum(m.apply_num), 0)
            into var_agent_order_count
            from psi_flow_apply t, psi_flow_apply_batch m
           where m.Supplier_Id = var_supplier_id
             and m.res_type_id = var_res_type_id
             and t.apply_type in
                 ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
                --and t.flow_id = m.flow_id and flow_status not in ('撤销','已发货');
             and t.flow_id = m.flow_id
             and flow_status not in
                 (select dictid
                    from dict_item
                   where groupid = 'apply_flow_status4');
      dbms_output.put_line('-->当 var_manual_flag !=  MANUAL：var_agent_order_count='||var_agent_order_count);
          --5.  获取供货商自有渠道此机型订单（状态为市市场部审批通过、市其他领导审批、省领导审批、供货平台确认、未发货完毕的订单量总和）
          select nvl(sum(m.apply_num), 0)
            into var_self_order_count
            from psi_flow_apply t, psi_flow_apply_batch m
           where m.Supplier_Id = var_supplier_id
             and m.res_type_id = var_res_type_id
             and t.apply_type in
                 ('CATA_MOBAPPLY_PUBLIC_SELL', 'CATA_MOBAPPLY_GROUP_SELL',
                  'CATA_MOBAPPLY_PUBLIC_SELL_A2S',
                  'CATA_MOBAPPLY_GROUP_SELL_A2S')
                --and t.flow_id = m.flow_id and flow_status in ('市场部审批通过','市其他领导审批','省领导审批','供货平台确认','未发货完毕');
             and t.flow_id = m.flow_id
             and flow_status in
                 (select dictid
                    from dict_item
                   where groupid = 'apply_flow_status5');
        dbms_output.put_line('-->当 var_manual_flag !=  MANUAL：var_self_order_count='||var_self_order_count);
          --6.  获取电子渠道订单量（平台已确认、未发货）
          select nvl(sum(m.apply_num), 0)
            into var_elec_order_count
            from psi_flow_apply t, psi_flow_apply_batch m
           where m.Supplier_Id = var_supplier_id
             and m.res_type_id = var_res_type_id
             and t.apply_type in ('CATA_MOBAPPLY_ECHANNEL_SELL')
                --and t.flow_id = m.flow_id and flow_status in ('平台已确认','未发货');
             and t.flow_id = m.flow_id
             and flow_status in
                 (select dictid
                    from dict_item
                   where groupid = 'apply_flow_status6');
      dbms_output.put_line('-->当 var_manual_flag !=  MANUAL：var_elec_order_count='||var_elec_order_count); 
          --7.  当前库存量=供货商实时库存-供货商社会渠道的此机型订单量-供货商自有渠道此机型订单-电子渠道订单量。
          var_brand_count := var_brand_count - var_agent_order_count -
                             var_self_order_count - var_elec_order_count;
       dbms_output.put_line('-->当 var_manual_flag !=  MANUAL：var_brand_count='||var_brand_count); 
          --8.  将此库存量更新到PSI_CFG_VENDOR_SUPPLYINFO表中。如果记录不存在则新增记录，并且手工标志为空。
          update PSI_CFG_VENDOR_SUPPLYINFO
             set left_num = var_brand_count
           where vendor_id = var_supplier_id
             and res_type_id = var_res_type_id;
          commit;
        end if;
      end if;
    end;
  end loop;
exception
  when others then
    rollback;
    dbms_output.put_line(' - >> sqlcode ' || SQLCODE || '
         ' || SQLERRM);
end loc_psi_cfg_supplier_res_prc;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_DAYPSI_TMP"
 IS
  p_statDay varchar2(32) := to_char(20170925);
begin
  --首先删除昨天的数据
  delete from psi_stat_daypsi t
   where t.stat_day = p_statDay
     and t.stat_type = 'COMMON';
  commit;
  for regionList in (select t.region from region_list t) loop
    dbms_output.put_line('-->regionList循环：region='||regionList.region);
    begin
      insert into psi_stat_daypsi
        (REGION,
         ORG_ID,
         RES_KIND_ID,
         STAT_DAY,
         STAT_TYPE,
         BRAND_ID,
         RES_TYPE_ID,
         SPECIAL_ATTR,
         OWNER_TYPE,
         SUPPLIER_ID,
         ORG_TYPE,
         SETTLE_MODE,
         LDSTORE_NUM,
         REFUND_NUM,
         ASIGIN_NUM,
         ASIGOUT_NUM,
         SALE_NUM,
         SALEBACK_NUM,
         ADJUSTIN_NUM,
         ADJUSTOUT_NUM,
         EXCHGIN_NUM,
         EXCHGOUT_NUM,
         STOCK_NUM,
         CREATE_DATE)
        select regionList.Region region,
               org_id,
               'rsclM' res_kind_id,
               p_statDay stat_day,
               'COMMON' stat_type,
               (select m.brand_id
                  from im_res_type m
                 where m.res_type_id = tt.res_type_id) brand_id,
               res_type_id,
               special_attr,
               owner_type,
               supplier_id,
               QRYCHANNELTYPE(tt.org_id, regionList.Region) org_type,
               settle_mode,
               max(ldstore_num) ldstore_num,
               max(refund_num) refund_num,
               max(asigin_num) asigin_num,
               max(asigout_num) asigout_num,
               max(sale_num) sale_num,
               max(saleback_num) saleback_num,
               0 adjustin_num,
               0 adjustout_num,
               0 exchgin_num,
               0 exchgout_num,
               nvl((select sum(n.stock_num)
                     from psi_stk_realtime_mstock n
                    where n.region = regionList.Region
                      and n.org_id = tt.org_id
                      and n.res_type_id = tt.res_type_id
                      and n.inv_status = 'INSTORE'
                      and nvl(n.special_attr, 'null') =
                          nvl(tt.special_attr, 'null')
                      and nvl(n.owner_type, 'null') =
                          nvl(tt.owner_type, 'null')
                      and nvl(n.settle_mode, 'null') =
                          nvl(tt.settle_mode, 'null')
                      and nvl(n.supplier_id, 'null') =
                          nvl(tt.supplier_id, 'null')),
                   0) stock_num,
               sysdate create_date
          from (select org_id,
                       res_type_id,
                       special_attr,
                       supplier_id,
                       owner_type,
                       settle_mode,
                       decode(statDaypsiType, 'LDSTORE_NUM', amount, 0) ldstore_num,
                       decode(statDaypsiType, 'REFUND_NUM', amount, 0) refund_num,
                       decode(statDaypsiType, 'ASIGIN_NUM', amount, 0) asigin_num,
                       decode(statDaypsiType, 'ASIGOUT_NUM', amount, 0) asigout_num,
                       decode(statDaypsiType, 'SALE_NUM', amount, 0) sale_num,
                       decode(statDaypsiType, 'SALEBACK_NUM', amount, 0) saleback_num
                  from (select t.org_id,
                               t.res_type_id,
                               t.special_attr,
                               t.supplier_id,
                               t.owner_type,
                               t.settle_mode,
                               psi_getStatDaypsiType(t.inout_flag,
                                                       t.busi_type,
                                                       t.sub_busi_type) statDaypsiType,
                               sum(t.amount) amount
                          from psi_busi_stock_log_his t
                         where t.region = regionList.Region
                           and t.res_kind_id = 'rsclM'
                           and t.create_date > to_date(p_statDay, 'yyyymmdd')
                           and t.create_date <
                               to_date(p_statDay, 'yyyymmdd') + 1
                           and t.oid like
                               to_char(to_date(p_statDay, 'yyyymmdd'),
                                       'yymmdd') || '%'
                           and psi_getStatDaypsiType(t.inout_flag,
                                                       t.busi_type,
                                                       t.sub_busi_type) is not null
                         group by t.org_id,
                                  t.res_type_id,
                                  t.special_attr,
                                  t.supplier_id,
                                  t.owner_type,
                                  t.settle_mode,
                                  psi_getStatDaypsiType(t.inout_flag,
                                                          t.busi_type,
                                                          t.sub_busi_type))) tt
         group by org_id,
                  res_type_id,
                  special_attr,
                  supplier_id,
                  owner_type,
                  settle_mode;
    dbms_output.put_line('-->表 psi_stat_daypsi 数据插入成功');
      --更新期初数量： 当前库存 - 入库量 - 调入量 - 销售回退量 + 退库量 + 调出量 + 销售量
      update psi_stat_daypsi t
         set t.original_num = t.stock_num - t.ldstore_num - t.asigin_num -
                              t.saleback_num + t.refund_num + t.asigout_num +
                              t.sale_num
       where t.region = regionList.Region
         and t.res_kind_id = 'rsclM'
         and t.stat_type = 'COMMON'
         and t.stat_day = p_statDay;
    dbms_output.put_line('-->表 psi_stat_daypsi 数据更新成功');
      commit;
    end;
  end loop;
end;
/

CREATE OR REPLACE PROCEDURE "PROC_INV_STAT_DAYPSI"
 IS
  p_statDay varchar2(32) := to_char(to_date('20170926','yyyy-mm-dd') - 1, 'YYYYMMDD');
begin
  --首先删除昨天的数据
  delete from psi_stat_daypsi t
   where t.stat_day = p_statDay
     and t.stat_type = 'COMMON';
  commit;
  for regionList in (select t.region from region_list t) loop
    dbms_output.put_line('-->regionList循环：region='||regionList.region);
    begin
      insert into psi_stat_daypsi
        (REGION,
         ORG_ID,
         RES_KIND_ID,
         STAT_DAY,
         STAT_TYPE,
         BRAND_ID,
         RES_TYPE_ID,
         SPECIAL_ATTR,
         OWNER_TYPE,
         SUPPLIER_ID,
         ORG_TYPE,
         SETTLE_MODE,
         LDSTORE_NUM,
         REFUND_NUM,
         ASIGIN_NUM,
         ASIGOUT_NUM,
         SALE_NUM,
         SALEBACK_NUM,
         ADJUSTIN_NUM,
         ADJUSTOUT_NUM,
         EXCHGIN_NUM,
         EXCHGOUT_NUM,
         STOCK_NUM,
         CREATE_DATE)
        select regionList.Region region,
               org_id,
               'rsclM' res_kind_id,
               p_statDay stat_day,
               'COMMON' stat_type,
               (select m.brand_id
                  from im_res_type m
                 where m.res_type_id = tt.res_type_id) brand_id,
               res_type_id,
               special_attr,
               owner_type,
               supplier_id,
               QRYCHANNELTYPE(tt.org_id, regionList.Region) org_type,
               settle_mode,
               max(ldstore_num) ldstore_num,
               max(refund_num) refund_num,
               max(asigin_num) asigin_num,
               max(asigout_num) asigout_num,
               max(sale_num) sale_num,
               max(saleback_num) saleback_num,
               0 adjustin_num,
               0 adjustout_num,
               0 exchgin_num,
               0 exchgout_num,
               nvl((select sum(n.stock_num)
                     from psi_stk_realtime_mstock n
                    where n.region = regionList.Region
                      and n.org_id = tt.org_id
                      and n.res_type_id = tt.res_type_id
                      and n.inv_status = 'INSTORE'
                      and nvl(n.special_attr, 'null') =
                          nvl(tt.special_attr, 'null')
                      and nvl(n.owner_type, 'null') =
                          nvl(tt.owner_type, 'null')
                      and nvl(n.settle_mode, 'null') =
                          nvl(tt.settle_mode, 'null')
                      and nvl(n.supplier_id, 'null') =
                          nvl(tt.supplier_id, 'null')),
                   0) stock_num,
               sysdate create_date
          from (select org_id,
                       res_type_id,
                       special_attr,
                       supplier_id,
                       owner_type,
                       settle_mode,
                       decode(statDaypsiType, 'LDSTORE_NUM', amount, 0) ldstore_num,
                       decode(statDaypsiType, 'REFUND_NUM', amount, 0) refund_num,
                       decode(statDaypsiType, 'ASIGIN_NUM', amount, 0) asigin_num,
                       decode(statDaypsiType, 'ASIGOUT_NUM', amount, 0) asigout_num,
                       decode(statDaypsiType, 'SALE_NUM', amount, 0) sale_num,
                       decode(statDaypsiType, 'SALEBACK_NUM', amount, 0) saleback_num
                  from (select t.org_id,
                               t.res_type_id,
                               t.special_attr,
                               t.supplier_id,
                               t.owner_type,
                               t.settle_mode,
                               psi_getStatDaypsiType(t.inout_flag,
                                                       t.busi_type,
                                                       t.sub_busi_type) statDaypsiType,
                               sum(t.amount) amount
                          from psi_busi_stock_log_his t
                         where t.region = regionList.Region
                           and t.res_kind_id = 'rsclM'
                           and t.create_date > to_date(p_statDay, 'yyyymmdd')
                           and t.create_date <
                               to_date(p_statDay, 'yyyymmdd') + 1
                           and t.oid like
                               to_char(to_date(p_statDay, 'yyyymmdd'),
                                       'yymmdd') || '%'
                           and psi_getStatDaypsiType(t.inout_flag,
                                                       t.busi_type,
                                                       t.sub_busi_type) is not null
                         group by t.org_id,
                                  t.res_type_id,
                                  t.special_attr,
                                  t.supplier_id,
                                  t.owner_type,
                                  t.settle_mode,
                                  psi_getStatDaypsiType(t.inout_flag,
                                                          t.busi_type,
                                                          t.sub_busi_type))) tt
         group by org_id,
                  res_type_id,
                  special_attr,
                  supplier_id,
                  owner_type,
                  settle_mode;

      --更新期初数量： 当前库存 - 入库量 - 调入量 - 销售回退量 + 退库量 + 调出量 + 销售量
      --生活潮品销售情况
     insert into psi_stat_daypsi
        (REGION,
         ORG_ID,
         RES_KIND_ID,
         STAT_DAY,
         STAT_TYPE,
         BRAND_ID,
         RES_TYPE_ID,
         SPECIAL_ATTR,
         OWNER_TYPE,
         SUPPLIER_ID,
         ORG_TYPE,
         SETTLE_MODE,
         LDSTORE_NUM,
         REFUND_NUM,
         ASIGIN_NUM,
         ASIGOUT_NUM,
         SALE_NUM,
         SALEBACK_NUM,
         ADJUSTIN_NUM,
         ADJUSTOUT_NUM,
         EXCHGIN_NUM,
         EXCHGOUT_NUM,
         STOCK_NUM,
         CREATE_DATE)
        select regionList.Region region,
               org_id,
               'rsclU' res_kind_id,
               p_statDay stat_day,
               'COMMON' stat_type,
               (select m.brand_id
                  from im_res_type m
                 where m.res_type_id = tt.res_type_id) brand_id,
               res_type_id,
               special_attr,
               '' owner_type,
               '' supplier_id,
               QRYCHANNELTYPE(tt.org_id, regionList.Region) org_type,
               '' settle_mode,
               max(ldstore_num) ldstore_num,
               max(refund_num) refund_num,
               max(asigin_num) asigin_num,
               max(asigout_num) asigout_num,
               max(sale_num) sale_num,
               max(saleback_num) saleback_num,
               max(timeoutrecover_num) adjustin_num,
               max(timeoutdel_num) adjustout_num,
               0 exchgin_num,
               0 exchgout_num,
               nvl(( SELECT sum(n.STORE_AMOUNT)
                    FROM IM_INV_UNSIGN n
                    WHERE n.region = regionList.Region
                      and n.org_id = tt.org_id
                      and n.res_type_id = tt.res_type_id
                     AND n.INV_STATUS = 'INSTORE'
                     AND n.BUSI_STATUS = 'USABLE'),
                   0) stock_num,
               sysdate create_date
          from (select org_id,
                       res_type_id,
                       special_attr,
                       decode(statDaypsiType, 'LDSTORE_NUM', amount, 0) ldstore_num,
                       decode(statDaypsiType, 'REFUND_NUM', amount, 0) refund_num,
                       decode(statDaypsiType, 'ASIGIN_NUM', amount, 0) asigin_num,
                       decode(statDaypsiType, 'ASIGOUT_NUM', amount, 0) asigout_num,
                       decode(statDaypsiType, 'SALE_NUM', amount, 0) sale_num,
                       decode(statDaypsiType, 'SALEBACK_NUM', amount, 0) saleback_num,
                       decode(statDaypsiType, 'TIMEOUTDEL_NUM', amount, 0) timeoutdel_num,
                       decode(statDaypsiType, 'TIMEOUTRECOVER_NUM', amount, 0) timeoutrecover_num
                  from (select a.org_id,
                               c.res_type_id,
                               c.special_attr,
                               psi_getStatDaypsiType(b.inout_flag,
                                                       a.busi_type,
                                                       '') statDaypsiType,
                               sum(c.assign_amount) amount
                          from im_busi_opera a,im_inout_store b,im_inout_store_amount c
                         where a.region = regionList.Region
                   and a.res_kind_id='rsclU'
                   and a.busi_type in ('SALE','SALE_BACK')
                 and a.busi_id like
                               to_char(to_date(p_statDay, 'yyyymmdd'),
                                       'yymmdd') || '%'
                 and b.busi_id = a.busi_id
                 and c.busi_id=a.busi_id
                 and c.res_type_id like 'rsclUN%'
                 and c.create_date > to_date(p_statDay, 'yyyymmdd')
                               and c.create_date <
                               to_date(p_statDay, 'yyyymmdd') + 1
                 and psi_getStatDaypsiType(b.inout_flag,
                                                       a.busi_type,
                                                       '') is not null
                         group by a.org_id,
                               c.res_type_id,
                               c.special_attr,
                                 psi_getStatDaypsiType(b.inout_flag,
                                                       a.busi_type,
                                                       '') )) tt
         group by org_id,
                  res_type_id,
                  special_attr;
    dbms_output.put_line('-->表 psi_stat_daypsi 数据插入成功');
      --生活潮品更新期初数量： 当前库存 - 入库量 - 调入量 - 销售回退量 + 退库量 + 调出量 + 销售量
      update psi_stat_daypsi t
         set t.original_num = t.stock_num - t.ldstore_num - t.asigin_num -
                              t.saleback_num + t.refund_num + t.asigout_num +
                              t.sale_num
       where t.region = regionList.Region
         and t.res_kind_id = 'rsclU'
         and t.stat_type = 'COMMON'
         and t.stat_day = p_statDay;
      update psi_stat_daypsi t
         set t.original_num = t.stock_num - t.ldstore_num - t.asigin_num -
                              t.saleback_num + t.refund_num + t.asigout_num +
                              t.sale_num
       where t.region = regionList.Region
         and t.res_kind_id = 'rsclM'
         and t.stat_type = 'COMMON'
         and t.stat_day = p_statDay;
     dbms_output.put_line('-->表 psi_stat_daypsi 数据更新成功');
      commit;
    end;
  end loop;
end;
/

CREATE OR REPLACE PROCEDURE "AP_RESPSI_MOBSETTLE" is
  v_stockprice     number; --买断价
  v_stockpriceNum  number;
  v_selfpricerate  number; --铺货价比
  v_num            number; --铺货价比数量
  v_num1           number; --铺货价数量
  v_selfprice      number; --铺货价
  v_selfprice_temp number; --铺货价临时变量
begin
  for c_in in (SELECT T.REGION,
                      T.INV_ID,
                      T.RES_TYPE_ID,
                      (SELECT M.BRAND_ID
                         FROM IM_RES_TYPE M
                        WHERE M.RES_TYPE_ID = T.RES_TYPE_ID) brand_id,
                      T.STATUS_DATE,
                      (SELECT N.INV_STATUS
                         FROM IM_INV_MOBTEL N
                        WHERE N.INV_ID = T.INV_ID
                          AND N.REGION = T.REGION) STATUS, --换货判断入和出
                      T.BUSI_ID,
                      (SELECT C.BUSI_TYPE
                         FROM IM_BUSI_OPERA C
                        WHERE C.BUSI_ID = T.BUSI_ID
                          AND C.RES_KIND_ID = T.RES_KIND_ID
                          AND C.REGION = T.REGION) BUSI_TYPE, --区分销售，退货，换货
                      (SELECT N.ORG_ID
                         FROM IM_INV_MOBTEL N
                        WHERE N.INV_ID = T.INV_ID
                          AND N.REGION = T.REGION) ORG_ID,
                      NVL(T.OPER_ORG,
                          (SELECT N.ORG_ID
                             FROM IM_INV_MOBTEL N
                            WHERE N.INV_ID = T.INV_ID
                              AND N.REGION = T.REGION)) SALE_ORG_ID,
                      (SELECT N.SUPPLIER_ID
                         FROM IM_INV_MOBTEL N
                        WHERE N.INV_ID = T.INV_ID
                          AND N.REGION = T.REGION) SUPPLIER_ID
                 FROM IM_SALE_DETAIL T
                WHERE T.RES_KIND_ID = 'rsclM'
                  AND T.STATUS_DATE >=
                      to_date(to_char(sysdate - 1, 'YYYYMMDD'), 'YYYYMMDD')
                  AND T.STATUS_DATE < TO_DATE('20301020','yyyy-mm-dd')
                  AND EXISTS (SELECT 1
                         FROM IM_INV_MOBTEL C
                        WHERE C.INV_ID = T.INV_ID
                          AND C.SETTLE_MODE = 'SELFSALE')) loop
    begin
   dbms_output.put_line('-->c_in循环：REGION='||c_in.REGION||
                    ' INV_ID='||c_in.INV_ID||
                    ' RES_TYPE_ID='||c_in.RES_TYPE_ID||
                    ' BRAND_ID='||c_in.BRAND_ID||
                    ' STATUS_DATE='||c_in.STATUS_DATE||
                    ' STATUS='||c_in.STATUS||
                    ' BUSI_ID='||c_in.BUSI_ID||
                    ' BUSI_TYPE='||c_in.BUSI_TYPE||
                    ' ORG_ID='||c_in.ORG_ID||
                    ' SALE_ORG_ID='||c_in.SALE_ORG_ID||
                    ' SUPPLIER_ID='||c_in.SUPPLIER_ID);
      --当前单位机型的买断价
      SELECT count(1)
        into v_stockpriceNum
        FROM RESTYPE_SALEPRICE A, PSI_ORGANIZATION B
       WHERE A.RES_TYPE_ID = c_in.res_type_id
         and a.orgid in (SELECT orgid
                           FROM PSI_ORGANIZATION
                          where status = 1
                          START WITH orgid = c_in.org_id
                         CONNECT BY PRIOR parentid = orgid)
         and a.status = '1'
         and nvl(a.starttime, sysdate - 1) < sysdate
         and nvl(a.endtime, sysdate + 1) > sysdate
         and nvl(a.pricetype, 'retail') = 'stock'
         and a.orgid = b.orgid
       order by b.orgLevel ASC;
     dbms_output.put_line('-->c_in循环：v_stockpriceNum='||v_stockpriceNum);

      if v_stockpriceNum > 0 then
        SELECT nvl(PRICE, 0)
          into v_stockprice
          FROM RESTYPE_SALEPRICE A, PSI_ORGANIZATION B
         WHERE A.RES_TYPE_ID = c_in.res_type_id
           and a.orgid in (SELECT orgid
                             FROM PSI_ORGANIZATION
                            where status = 1
                            START WITH orgid = c_in.org_id
                           CONNECT BY PRIOR parentid = orgid)
           and a.status = '1'
           and nvl(a.starttime, sysdate - 1) < sysdate
           and nvl(a.endtime, sysdate + 1) > sysdate
           and nvl(a.pricetype, 'retail') = 'stock'
           and a.orgid = b.orgid
         order by b.orgLevel ASC;
        --当前单位机型的铺货价比
        select count(1)
          into v_num
          from psi_cfg_selfsale_pricerate pc
         where pc.res_type_id = c_in.res_type_id
           and pc.org_id = c_in.org_id
           and pc.eff_date <= sysdate
           and pc.exp_date >= sysdate;

        if v_num > 0 then
          select pc.price_rate
            into v_selfpricerate
            from Psi_cfg_selfsale_pricerate pc
           where pc.res_type_id = c_in.res_type_id
             and pc.org_id = c_in.org_id
             and pc.eff_date <= sysdate
             and pc.exp_date >= sysdate;
        else
          v_selfpricerate := null;
        end if;
        if v_selfpricerate is not null then
          v_selfprice := round((1 + v_selfpricerate * 0.01) * v_stockprice);
        end if;
        --1.销售
        if c_in.busi_type = 'SALE' then
          if v_selfpricerate is not null then
            v_selfprice := v_selfprice;
          else
            v_selfprice := v_stockprice;
          end if;
        end if;

        --2.退货或回退
        if c_in.busi_type = 'UNSALE' then
          select count(1)
            into v_num1
            from psi_inv_mobsettle t
           where t.inv_id = c_in.inv_id;

          if v_num1 > 0 then
            select t.Settle_Price
              into v_selfprice_temp
              from Psi_inv_mobsettle t
             where t.inv_id = c_in.inv_id
               and rownum = 1;
            v_selfprice := -v_selfprice_temp;
          elsif v_selfpricerate is not null then
            v_selfprice := -v_selfprice;
          else
            v_selfprice := v_stockprice;
          end if;
        end if;
        --3.换机
        if c_in.busi_type = 'EXCHSALE' then
          if v_selfpricerate is not null and c_in.status = 'INSTORE' then
            v_selfprice := -v_selfprice;
          elsif v_selfpricerate is not null and c_in.status = 'OUTSTORE' then
            v_selfprice := v_selfprice;
          else
            v_selfprice := v_stockprice;
          end if;
        end if;
        insert into Psi_inv_mobsettle
          (region,
           Inv_Id,
           Brand_Id,
           Res_Type_Id,
           Rec_Date,
           Busi_Type,
           Busi_Id,
           Org_Id,
           Sale_Org_Id,
           Settle_Price,
           Supplier_Id)
        values
          (c_in.region,
           c_in.inv_id,
           c_in.brand_id,
           c_in.res_type_id,
           c_in.status_date,
           c_in.busi_type,
           c_in.busi_id,
           c_in.org_id,
           c_in.sale_org_id,
           nvl(v_selfprice, 0),
           c_in.supplier_id);
      end if;
    end;
    commit;
  end loop;
  commit;
end AP_RESPSI_MOBSETTLE;
/

CREATE OR REPLACE PROCEDURE "LOC_4G_SPRING_SELLRANKING" (I_PROJECTID IN VARCHAR2) IS
  V_ALLTERMINAL_AMOUNT NUMBER; --总订货量
BEGIN
  --计算总订货量
  SELECT SUM(A.SUM_APPLY_NUM)
    INTO V_ALLTERMINAL_AMOUNT
    FROM PSI_FLOW_APPLY A
   WHERE A.PROJECT_ID = I_PROJECTID
     AND A.APPLY_TYPE IN
         ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
     AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
     AND TRUNC(A.CREATE_DATE, 'dd') =
         TRUNC(SYSDATE, 'dd');
  DELETE FROM PSI_LOC_PROJ_STAT WHERE STAT_TYPE = 'MOB_MODEL';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_STAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     TERMINAL_AMOUNT,
     SUM_MONEY,
     PAID_MONEY,
     ORDE_CHANNNEL_AMOUNT,
     ORDER_AMOUNT_PERCENT,
     SUPPLIER_ID,
     CREATE_DATE)
    SELECT B.REGION REGION,
           B.RES_TYPE_ID ATTR_ID,
           'MOB_MODEL' STAT_TYPE,
           COUNT(DISTINCT B.FLOW_ID) ORDER_AMOUNT,
           SUM(B.APPLY_NUM) TERMINAL_AMOUNT,
           SUM(B.TOTAL_PRICE / 100) SUM_MONEY,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', B.TOTAL_PRICE / 100, 0)) PAID_MONEY,
           COUNT(DISTINCT A.ORG_ID) ORDE_CHANNNEL_AMOUNT,
           SUM(B.APPLY_NUM) / V_ALLTERMINAL_AMOUNT ORDER_AMOUNT_PERCENT,
           NVL(B.SUPPLIER_ID, '未知') SUPPLIER_ID,
           SYSDATE
      FROM PSI_FLOW_APPLY A, PSI_FLOW_APPLY_BATCH B
     WHERE A.PROJECT_ID = I_PROJECTID
       AND A.REGION = B.REGION
       AND A.FLOW_ID = B.FLOW_ID
       AND A.APPLY_TYPE IN
           ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
       AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (B.REGION, B.RES_TYPE_ID, B.SUPPLIER_ID);

  UPDATE PSI_LOC_PROJ_STAT T
     SET T.ATTR_NAME =
         (SELECT A.RES_TYPE_NAME
            FROM IM_RES_TYPE A
           WHERE T.ATTR_ID = A.RES_TYPE_ID
             AND A.STATUS = 'VALID'
             AND DECODE(A.EFF_DATE, NULL, SYSDATE - 1, A.EFF_DATE) <= SYSDATE
             AND DECODE(A.EXP_DATE, NULL, SYSDATE + 1, A.EXP_DATE) > SYSDATE
             AND ROWNUM = 1),
         T.BRAND_ID =
         (SELECT A.BRAND_ID
            FROM IM_RES_TYPE A
           WHERE T.ATTR_ID = A.RES_TYPE_ID
             AND A.STATUS = 'VALID'
             AND DECODE(A.EFF_DATE, NULL, SYSDATE - 1, A.EFF_DATE) <= SYSDATE
             AND DECODE(A.EXP_DATE, NULL, SYSDATE + 1, A.EXP_DATE) > SYSDATE
             AND ROWNUM = 1)
   WHERE T.STAT_TYPE = 'MOB_MODEL';
  COMMIT;

  --统计维度为品牌 (stat_type=’MOB_BRAND’)
  --先删除 stat_type=’MOB_BRAND’ 的数据
  DELETE FROM PSI_LOC_PROJ_STAT WHERE STAT_TYPE = 'MOB_BRAND';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_STAT
    (REGION,
     ATTR_ID,
     ATTR_NAME,
     STAT_TYPE,
     ORDER_AMOUNT,
     TERMINAL_AMOUNT,
     SUM_MONEY,
     PAID_MONEY,
     ORDE_CHANNNEL_AMOUNT,
     ORDER_AMOUNT_PERCENT,
     BRAND_ID,
     CREATE_DATE)
    SELECT REGION,
       ATTR_ID,
       ATTR_NAME,
       STAT_TYPE,
       COUNT(DISTINCT FLOW_ID) ORDER_AMOUNT,
       SUM(APPLY_NUM) TERMINAL_AMOUNT,
       SUM(TOTAL_PRICE) SUM_MONEY,
       SUM(DECODE(PAY_STATUS, 'PAYOVER', TOTAL_PRICE, 0)) PAID_MONEY,
       COUNT(DISTINCT ORG_ID) ORDE_CHANNNEL_AMOUNT,
       SUM(APPLY_NUM)/V_ALLTERMINAL_AMOUNT ORDER_AMOUNT_PERCENT,
       BRAND_ID,
       SYSDATE
  FROM (SELECT B.REGION REGION,
               (SELECT C.BRAND_ID
                  FROM PSI_CFG_RESBRAND C, IM_RES_TYPE D
                 WHERE C.BRAND_ID = D.BRAND_ID
                   AND D.RES_TYPE_ID = B.RES_TYPE_ID and rownum=1) ATTR_ID,
               (SELECT C.BRAND_NAME
                  FROM PSI_CFG_RESBRAND C, IM_RES_TYPE D
                 WHERE C.BRAND_ID = D.BRAND_ID
                   AND D.RES_TYPE_ID = B.RES_TYPE_ID and rownum=1) ATTR_NAME,
               'MOB_BRAND' STAT_TYPE,
               B.FLOW_ID FLOW_ID,
               B.APPLY_NUM APPLY_NUM,
               B.TOTAL_PRICE / 100 TOTAL_PRICE,
               A.PAY_STATUS PAY_STATUS,
               A.ORG_ID ORG_ID,
               (SELECT C.BRAND_ID
                  FROM PSI_CFG_RESBRAND C, IM_RES_TYPE D
                 WHERE C.BRAND_ID = D.BRAND_ID
                   AND D.RES_TYPE_ID = B.RES_TYPE_ID and rownum=1) BRAND_ID
          FROM PSI_FLOW_APPLY A, PSI_FLOW_APPLY_BATCH B
         WHERE A.PROJECT_ID = I_PROJECTID
           AND A.REGION = B.REGION
           AND A.FLOW_ID = B.FLOW_ID
           AND A.APPLY_TYPE IN
               ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
           AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
           AND TRUNC(A.CREATE_DATE, 'dd') =
               TRUNC(SYSDATE, 'dd'))
 GROUP BY REGION, ATTR_ID, ATTR_NAME, STAT_TYPE, BRAND_ID;
  COMMIT;

  --统计维度为平台 (stat_type=’SUPPLIER’)
  --先删除 stat_type=’SUPPLIER’ 的数据
  DELETE FROM PSI_LOC_PROJ_STAT WHERE STAT_TYPE = 'SUPPLIER';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_STAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     TERMINAL_AMOUNT,
     SUM_MONEY,
     PAID_MONEY,
     ORDE_CHANNNEL_AMOUNT,
     ORDER_AMOUNT_PERCENT,
     SUPPLIER_ID,
     CREATE_DATE)
    SELECT B.REGION REGION,
           NVL(B.SUPPLIER_ID, '未知') ATTR_ID,
           'SUPPLIER' STAT_TYPE,
           COUNT(DISTINCT B.FLOW_ID) ORDER_AMOUNT,
           SUM(B.APPLY_NUM) TERMINAL_AMOUNT,
           SUM(B.TOTAL_PRICE / 100) SUM_MONEY,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', B.TOTAL_PRICE / 100, 0)) PAID_MONEY,
           COUNT(DISTINCT A.ORG_ID) ORDE_CHANNNEL_AMOUNT,
           SUM(B.APPLY_NUM) / V_ALLTERMINAL_AMOUNT ORDER_AMOUNT_PERCENT,
           NVL(B.SUPPLIER_ID, '未知') SUPPLIER_ID,
           SYSDATE
      FROM PSI_FLOW_APPLY A, PSI_FLOW_APPLY_BATCH B
     WHERE A.PROJECT_ID = I_PROJECTID
       AND A.REGION = B.REGION
       AND A.FLOW_ID = B.FLOW_ID
       AND A.APPLY_TYPE IN
           ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
       AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (B.REGION, B.SUPPLIER_ID);

  UPDATE PSI_LOC_PROJ_STAT T
     SET T.ATTR_NAME =
         (SELECT A.PARTNER_NAME
            FROM PSI_INFO_PARTNER A
           WHERE T.ATTR_ID = A.PARTNER_ID
             AND A.PARTNER_TYPE = 'PMPSuppTerm'
             AND ROWNUM = 1)
   WHERE T.STAT_TYPE = 'SUPPLIER';
  COMMIT;

  --统计维度为小时 (stat_type=’HOURS_INTERVAL’)
  --先删除 stat_type=’HOURS_INTERVAL’ 的数据
  DELETE FROM PSI_LOC_PROJ_STAT WHERE STAT_TYPE = 'HOURS_INTERVAL';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_STAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     TERMINAL_AMOUNT,
     SUM_MONEY,
     PAID_MONEY,
     ORDER_AMOUNT_PERCENT,
     ORDE_CHANNNEL_AMOUNT,
     CREATE_DATE)
    SELECT A.REGION,
           TO_CHAR(A.CREATE_DATE, 'hh24') || '-' ||
           (TO_CHAR(A.CREATE_DATE+1/24, 'hh24')) ATTR_ID,
           'HOURS_INTERVAL' STAT_TYPE,
           COUNT(DISTINCT A.FLOW_ID) ORDER_AMOUNT,
           SUM(B.APPLY_NUM) TERMINAL_AMOUNT,
           SUM(B.TOTAL_PRICE / 100) SUM_MONEY,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', B.TOTAL_PRICE / 100, 0)) PAID_MONEY,
           SUM(B.APPLY_NUM) / V_ALLTERMINAL_AMOUNT ORDER_AMOUNT_PERCENT,
           COUNT(DISTINCT A.ORG_ID) ORDE_CHANNNEL_AMOUNT,
           SYSDATE
      FROM PSI_FLOW_APPLY A, PSI_FLOW_APPLY_BATCH B
     WHERE A.PROJECT_ID = I_PROJECTID
       AND A.FLOW_ID = B.FLOW_ID
       AND A.REGION = B.REGION
       AND A.APPLY_TYPE IN
           ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
       AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (A.REGION, A.CREATE_DATE);
  COMMIT;

  --统计维度为地市 (stat_type=’CITY’)
  --先删除 stat_type=’CITY’ 的数据
  DELETE FROM PSI_LOC_PROJ_STAT WHERE STAT_TYPE = 'CITY';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_STAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     TERMINAL_AMOUNT,
     SUM_MONEY,
     PAID_MONEY,
     ORDE_CHANNNEL_AMOUNT,
     ORDER_AMOUNT_PERCENT,
     CREATE_DATE)
    SELECT A.REGION,
           A.REGION ATTR_ID,
           'CITY' STAT_TYPE,
           COUNT(DISTINCT A.FLOW_ID) ORDER_AMOUNT,
           SUM(B.APPLY_NUM) TERMINAL_AMOUNT,
           SUM(B.TOTAL_PRICE / 100) SUM_MONEY,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', B.TOTAL_PRICE / 100, 0)) PAID_MONEY,
           COUNT(DISTINCT A.ORG_ID) ORDE_CHANNNEL_AMOUNT,
           SUM(B.APPLY_NUM) / V_ALLTERMINAL_AMOUNT ORDER_AMOUNT_PERCENT,
           SYSDATE
      FROM PSI_FLOW_APPLY A, PSI_FLOW_APPLY_BATCH B
     WHERE A.PROJECT_ID = I_PROJECTID
       AND A.FLOW_ID = B.FLOW_ID
       AND A.REGION = B.REGION
       AND A.APPLY_TYPE IN
           ('CATA_MOBAPPLY_AGENTSELL', 'CATA_MOBAPPLY_AGENTPRESELL')
       AND A.FLOW_STATUS IN ('UNCONSIGNMENT', 'FINISHED')
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (A.REGION);

  UPDATE PSI_LOC_PROJ_STAT T
     SET T.ATTR_NAME =
         (SELECT A.REGIONNAME
            FROM REGION_LIST A
           WHERE T.REGION = A.REGION
             AND ROWNUM = 1)
   WHERE T.STAT_TYPE = 'CITY';
  COMMIT;

  DELETE FROM PSI_LOC_PROJ_ORDERSTAT WHERE STAT_TYPE = 'SUPPLIER';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_ORDERSTAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     REJECT_AMOUNT,
     CANCEL_AMOUNT,
     UNAPPROVE_AMOUNT,
     APPROVED_AMOUNT,
     UNPAY_AMOUNT,
     PAID_AMOUNT,
     SEND_AMOUNT,
     RECEIVE_AMOUNT,
     COMPLETED_AMOUNT,
     SUPPLIER_ID,
     CREATE_DATE)
    SELECT A.REGION REGION,
           A.SRC_ORG_ID ATTR_ID,
           'SUPPLIER' STAT_TYPE,
           COUNT(1) ORDER_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('DISAPPROVEBYSUPP_CITY',
                                        'DISAPPROVEBYSTOCK_CITY',
                                        'DISAPPROVEBYSTOCK',
                                        'DISAPPROVEBYREGIONLEADER',
                                        'DISAPPROVEBYPROVMGR',
                                        'DISAPPROVEBYPROVLEADER',
                                        'DISAPPROVEBYPROVADM',
                                        'DISAPPROVEBYCOUNTYLEADER',
                                        'DISAPPROVEBYCOUN',
                                        'DISAPPROVEBYCITY',
                                        'DISAPPROVE',
                                        'DISAPPROVEBYPROV',
                                        'DISAPPROVEBYREGIONSTOCK',
                                        'DISAPPROVEBYSUPP',
                                        'PROV_CHECKUNPASS') THEN
                  1
                 ELSE
                  0
               END) REJECT_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'cancel', 1, 0)) CANCEL_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('UNAPPROVEBYSTOCK',
                                        'UNAPPROVEBYREGIONLEADER',
                                        'UNAPPROVEBYPROVMGR',
                                        'UNAPPROVEBYPROVLEADER',
                                        'UNAPPROVEBYCOUNTYLEADER',
                                        'UNAPPROVEBYCOUN',
                                        'UNAPPROVE',
                                        'UNAPPROVEBYREGION') THEN
                  1
                 ELSE
                  0
               END) UNAPPROVE_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('SUPAPPROVE', 'PSI_SUPPAUDIT') THEN
                  1
                 ELSE
                  0
               END) APPROVED_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'UNPAYMENT', 1, 0)) UNPAY_AMOUNT,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', 1, 0)) PAID_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) SEND_AMOUNT,
           SUM((SELECT COUNT(DISTINCT T.RELA_ID)
                 FROM PSI_FLOW_STOCKOUT T
                WHERE A.REGION = T.REGION
                  AND A.FLOW_ID = T.RELA_ID
                  AND T.STATUS = 'FINISHED')) RECEIVE_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) COMPLETED_AMOUNT,
           NVL(A.SRC_ORG_ID, '未知') SUPPLIER_ID,
           SYSDATE
      FROM PSI_FLOW_APPLY A
     WHERE A.PROJECT_ID = I_PROJECTID
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (A.REGION,A.SRC_ORG_ID);

  UPDATE PSI_LOC_PROJ_ORDERSTAT T
     SET T.ATTR_NAME =
         (SELECT A.PARTNER_NAME
            FROM PSI_INFO_PARTNER A
           WHERE T.ATTR_ID = A.PARTNER_ID
             AND A.PARTNER_TYPE = 'PMPSuppTerm'
             AND ROWNUM = 1)
   WHERE T.STAT_TYPE = 'SUPPLIER';
  COMMIT;

  --统计维度为小时 (stat_type=’HOURS_INTERVAL’)
  --先删除 stat_type=’HOURS_INTERVAL’ 的数据
  DELETE FROM PSI_LOC_PROJ_ORDERSTAT WHERE STAT_TYPE = 'HOURS_INTERVAL';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_ORDERSTAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     REJECT_AMOUNT,
     CANCEL_AMOUNT,
     UNAPPROVE_AMOUNT,
     APPROVED_AMOUNT,
     UNPAY_AMOUNT,
     PAID_AMOUNT,
     SEND_AMOUNT,
     RECEIVE_AMOUNT,
     COMPLETED_AMOUNT,
     SUPPLIER_ID,
     CREATE_DATE)
    SELECT A.REGION,
           TO_CHAR(A.CREATE_DATE, 'hh24') || '-' ||
           (TO_CHAR(A.CREATE_DATE+1/24, 'hh24')) ATTR_ID,
           'HOURS_INTERVAL' STAT_TYPE,
           COUNT(1) ORDER_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('DISAPPROVEBYSUPP_CITY',
                                        'DISAPPROVEBYSTOCK_CITY',
                                        'DISAPPROVEBYSTOCK',
                                        'DISAPPROVEBYREGIONLEADER',
                                        'DISAPPROVEBYPROVMGR',
                                        'DISAPPROVEBYPROVLEADER',
                                        'DISAPPROVEBYPROVADM',
                                        'DISAPPROVEBYCOUNTYLEADER',
                                        'DISAPPROVEBYCOUN',
                                        'DISAPPROVEBYCITY',
                                        'DISAPPROVE',
                                        'DISAPPROVEBYPROV',
                                        'DISAPPROVEBYREGIONSTOCK',
                                        'DISAPPROVEBYSUPP',
                                        'PROV_CHECKUNPASS') THEN
                  1
                 ELSE
                  0
               END) REJECT_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'cancel', 1, 0)) CANCEL_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('UNAPPROVEBYSTOCK',
                                        'UNAPPROVEBYREGIONLEADER',
                                        'UNAPPROVEBYPROVMGR',
                                        'UNAPPROVEBYPROVLEADER',
                                        'UNAPPROVEBYCOUNTYLEADER',
                                        'UNAPPROVEBYCOUN',
                                        'UNAPPROVE',
                                        'UNAPPROVEBYREGION') THEN
                  1
                 ELSE
                  0
               END) UNAPPROVE_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('SUPAPPROVE', 'PSI_SUPPAUDIT') THEN
                  1
                 ELSE
                  0
               END) APPROVED_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'UNPAYMENT', 1, 0)) UNPAY_AMOUNT,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', 1, 0)) PAID_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) SEND_AMOUNT,
           SUM((SELECT COUNT(DISTINCT T.RELA_ID)
                 FROM PSI_FLOW_STOCKOUT T
                WHERE A.REGION = T.REGION
                  AND A.FLOW_ID = T.RELA_ID
                  AND T.STATUS = 'FINISHED')) RECEIVE_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) COMPLETED_AMOUNT,
           NVL(A.SRC_ORG_ID, '未知') SUPPLIER_ID,
           SYSDATE
      FROM PSI_FLOW_APPLY A
     WHERE A.PROJECT_ID = I_PROJECTID
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (A.REGION, A.CREATE_DATE, A.SRC_ORG_ID);
  COMMIT;

  --统计维度为地市 (stat_type=’CITY’)
  --先删除 stat_type=’CITY’ 的数据
  DELETE FROM PSI_LOC_PROJ_ORDERSTAT WHERE STAT_TYPE = 'CITY';
  --插入新的数据
  INSERT INTO PSI_LOC_PROJ_ORDERSTAT
    (REGION,
     ATTR_ID,
     STAT_TYPE,
     ORDER_AMOUNT,
     REJECT_AMOUNT,
     CANCEL_AMOUNT,
     UNAPPROVE_AMOUNT,
     APPROVED_AMOUNT,
     UNPAY_AMOUNT,
     PAID_AMOUNT,
     SEND_AMOUNT,
     RECEIVE_AMOUNT,
     COMPLETED_AMOUNT,
     CREATE_DATE)
    SELECT A.REGION,
           A.REGION ATTR_ID,
           'CITY' STAT_TYPE,
           COUNT(1) ORDER_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('DISAPPROVEBYSUPP_CITY',
                                        'DISAPPROVEBYSTOCK_CITY',
                                        'DISAPPROVEBYSTOCK',
                                        'DISAPPROVEBYREGIONLEADER',
                                        'DISAPPROVEBYPROVMGR',
                                        'DISAPPROVEBYPROVLEADER',
                                        'DISAPPROVEBYPROVADM',
                                        'DISAPPROVEBYCOUNTYLEADER',
                                        'DISAPPROVEBYCOUN',
                                        'DISAPPROVEBYCITY',
                                        'DISAPPROVE',
                                        'DISAPPROVEBYPROV',
                                        'DISAPPROVEBYREGIONSTOCK',
                                        'DISAPPROVEBYSUPP',
                                        'PROV_CHECKUNPASS') THEN
                  1
                 ELSE
                  0
               END) REJECT_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'cancel', 1, 0)) CANCEL_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('UNAPPROVEBYSTOCK',
                                        'UNAPPROVEBYREGIONLEADER',
                                        'UNAPPROVEBYPROVMGR',
                                        'UNAPPROVEBYPROVLEADER',
                                        'UNAPPROVEBYCOUNTYLEADER',
                                        'UNAPPROVEBYCOUN',
                                        'UNAPPROVE',
                                        'UNAPPROVEBYREGION') THEN
                  1
                 ELSE
                  0
               END) UNAPPROVE_AMOUNT,
           SUM(CASE
                 WHEN A.FLOW_STATUS IN ('SUPAPPROVE', 'PSI_SUPPAUDIT') THEN
                  1
                 ELSE
                  0
               END) APPROVED_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'UNPAYMENT', 1, 0)) UNPAY_AMOUNT,
           SUM(DECODE(A.PAY_STATUS, 'PAYOVER', 1, 0)) PAID_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) SEND_AMOUNT,
           SUM((SELECT COUNT(DISTINCT T.RELA_ID)
                 FROM PSI_FLOW_STOCKOUT T
                WHERE A.REGION = T.REGION
                  AND A.FLOW_ID = T.RELA_ID
                  AND T.STATUS = 'FINISHED')) RECEIVE_AMOUNT,
           SUM(DECODE(A.FLOW_STATUS, 'FINISHED', 1, 0)) COMPLETED_AMOUNT,
           SYSDATE
      FROM PSI_FLOW_APPLY A
     WHERE A.PROJECT_ID = I_PROJECTID
       AND TRUNC(A.CREATE_DATE, 'dd') =
           TRUNC(SYSDATE, 'dd')
     GROUP BY (A.REGION);

  UPDATE PSI_LOC_PROJ_ORDERSTAT T
     SET T.ATTR_NAME =
         (SELECT A.REGIONNAME
            FROM REGION_LIST A
           WHERE T.REGION = A.REGION
             AND ROWNUM = 1)
   WHERE T.STAT_TYPE = 'CITY';
  COMMIT;

  --订货会订单统计数据报表 end
END LOC_4G_SPRING_SELLRANKING;
/
--复杂类型声明，复杂类型赋值
--1024个tb类型变量，t2:=t1;t3:=t2;t4:=t3...
declare
v_sql varchar2(30000):='create or replace procedure p_composite_var_1 is t0 tb:=tb(1);';
begin
for i in 1..1024 loop
  v_sql :=v_sql ||'t'||i ||' tb:=tb();';
end loop;
v_sql := v_sql || ' begin ';
for i in 1..1024 loop
  v_sql :=v_sql ||'t'||i ||' :=t'||(i-1)||';';
end loop;
v_sql:=v_sql||'end;';
execute immediate v_sql;
end;
/
--512个tb类型变量，select 1 bulk collect into t0;select 1 bulk collect into t2 from t0;...
declare
v_sql varchar2(30000):='create or replace procedure p_composite_var_2 is t0 tb:=tb(1);';
begin
for i in 1..512 loop
  v_sql :=v_sql ||'t'||i ||' tb:=tb();';
end loop;
v_sql := v_sql || ' begin ';
for i in 1..512 loop
  v_sql :=v_sql ||'select 1 bulk collect into t'||i||' from t0;';
end loop;
v_sql:=v_sql||'end;';
execute immediate v_sql;
end;
/
--512个tb类型变量，p0(t1);p0(t2)....通过procedure出参来赋值
declare
v_sql varchar2(30000):='create or replace procedure p_composite_var_3 is t0 tb:=tb(1);procedure p0(a out tb) is begin a:=tb(0);a.extend(100,1);end;';
begin
for i in 1..512 loop
  v_sql :=v_sql ||'t'||i ||' tb:=tb();';
end loop;
v_sql := v_sql || ' begin ';
for i in 1..512 loop
  v_sql :=v_sql ||'p0(t'||i||');';
end loop;
v_sql:=v_sql||'end;';
execute immediate v_sql;
end;
/
create or replace procedure p_composite_var_4(i int) is
var1 obj_typ_nst1:=obj_typ_nst1(i);
var2 obj_typ_nst2:=obj_typ_nst2(var1);
var3 obj_typ_nst3:=obj_typ_nst3(var2);
var4 obj_typ_nst4:=obj_typ_nst4(var3);
var5 obj_typ_nst5:=obj_typ_nst5(var4);
var6 obj_typ_nst6:=obj_typ_nst6(var5);
var7 obj_typ_nst7:=obj_typ_nst7(var6);
var8 obj_typ_nst8:=obj_typ_nst8(var7);
var9 obj_typ_nst9:=obj_typ_nst9(var8);
var10 obj_typ_nst10:=obj_typ_nst10(var9);
var11 obj_typ_nst11:=obj_typ_nst11(var10);
var12 obj_typ_nst12:=obj_typ_nst12(var11);
var13 obj_typ_nst13:=obj_typ_nst13(var12);
var14 obj_typ_nst14:=obj_typ_nst14(var13);
var15 obj_typ_nst15:=obj_typ_nst15(var14);
var16 obj_typ_nst16:=obj_typ_nst16(var15);
var17 obj_typ_nst17:=obj_typ_nst17(var16);
var18 obj_typ_nst18:=obj_typ_nst18(var17);
var19 obj_typ_nst19:=obj_typ_nst19(var18);
var20 obj_typ_nst20:=obj_typ_nst20(var19);
var21 obj_typ_nst21:=obj_typ_nst21(var20);
var22 obj_typ_nst22:=obj_typ_nst22(var21);
var23 obj_typ_nst23:=obj_typ_nst23(var22);
var24 obj_typ_nst24:=obj_typ_nst24(var23);
var25 obj_typ_nst25:=obj_typ_nst25(var24);
var26 obj_typ_nst26:=obj_typ_nst26(var25);
var27 obj_typ_nst27:=obj_typ_nst27(var26);
var28 obj_typ_nst28:=obj_typ_nst28(var27);
var29 obj_typ_nst29:=obj_typ_nst29(var28);
var30 obj_typ_nst30:=obj_typ_nst30(var29);
var31 obj_typ_nst31:=obj_typ_nst31(var30);
var32 obj_typ_nst32:=obj_typ_nst32(var31);
var33 obj_typ_nst33:=obj_typ_nst33(var32);
var34 obj_typ_nst34:=obj_typ_nst34(var33);
var35 obj_typ_nst35:=obj_typ_nst35(var34);
var36 obj_typ_nst36:=obj_typ_nst36(var35);
var37 obj_typ_nst37:=obj_typ_nst37(var36);
var38 obj_typ_nst38:=obj_typ_nst38(var37);
var39 obj_typ_nst39:=obj_typ_nst39(var38);
var40 obj_typ_nst40:=obj_typ_nst40(var39);
var41 obj_typ_nst41:=obj_typ_nst41(var40);
var42 obj_typ_nst42:=obj_typ_nst42(var41);
var43 obj_typ_nst43:=obj_typ_nst43(var42);
var44 obj_typ_nst44:=obj_typ_nst44(var43);
var45 obj_typ_nst45:=obj_typ_nst45(var44);
var46 obj_typ_nst46:=obj_typ_nst46(var45);
var47 obj_typ_nst47:=obj_typ_nst47(var46);
var48 obj_typ_nst48:=obj_typ_nst48(var47);
var49 obj_typ_nst49:=obj_typ_nst49(var48);
var50 obj_typ_nst50:=obj_typ_nst50(var49);
var51 obj_typ_nst51:=obj_typ_nst51(var50);
var52 obj_typ_nst52:=obj_typ_nst52(var51);
var53 obj_typ_nst53:=obj_typ_nst53(var52);
var54 obj_typ_nst54:=obj_typ_nst54(var53);
var55 obj_typ_nst55:=obj_typ_nst55(var54);
var56 obj_typ_nst56:=obj_typ_nst56(var55);
var57 obj_typ_nst57:=obj_typ_nst57(var56);
var58 obj_typ_nst58:=obj_typ_nst58(var57);
var59 obj_typ_nst59:=obj_typ_nst59(var58);
var60 obj_typ_nst60:=obj_typ_nst60(var59);
var61 obj_typ_nst61:=obj_typ_nst61(var60);
var62 obj_typ_nst62:=obj_typ_nst62(var61);
var63 obj_typ_nst63:=obj_typ_nst63(var62);
var64 obj_typ_nst64:=obj_typ_nst64(var63);
var65 obj_typ_nst65:=obj_typ_nst65(var64);
var66 obj_typ_nst66:=obj_typ_nst66(var65);
var67 obj_typ_nst67:=obj_typ_nst67(var66);
var68 obj_typ_nst68:=obj_typ_nst68(var67);
var69 obj_typ_nst69:=obj_typ_nst69(var68);
var70 obj_typ_nst70:=obj_typ_nst70(var69);
var71 obj_typ_nst71:=obj_typ_nst71(var70);
var72 obj_typ_nst72:=obj_typ_nst72(var71);
var73 obj_typ_nst73:=obj_typ_nst73(var72);
var74 obj_typ_nst74:=obj_typ_nst74(var73);
var75 obj_typ_nst75:=obj_typ_nst75(var74);
var76 obj_typ_nst76:=obj_typ_nst76(var75);
var77 obj_typ_nst77:=obj_typ_nst77(var76);
var78 obj_typ_nst78:=obj_typ_nst78(var77);
var79 obj_typ_nst79:=obj_typ_nst79(var78);
var80 obj_typ_nst80:=obj_typ_nst80(var79);
var81 obj_typ_nst81:=obj_typ_nst81(var80);
var82 obj_typ_nst82:=obj_typ_nst82(var81);
var83 obj_typ_nst83:=obj_typ_nst83(var82);
var84 obj_typ_nst84:=obj_typ_nst84(var83);
var85 obj_typ_nst85:=obj_typ_nst85(var84);
var86 obj_typ_nst86:=obj_typ_nst86(var85);
var87 obj_typ_nst87:=obj_typ_nst87(var86);
var88 obj_typ_nst88:=obj_typ_nst88(var87);
var89 obj_typ_nst89:=obj_typ_nst89(var88);
var90 obj_typ_nst90:=obj_typ_nst90(var89);
var91 obj_typ_nst91:=obj_typ_nst91(var90);
var92 obj_typ_nst92:=obj_typ_nst92(var91);
var93 obj_typ_nst93:=obj_typ_nst93(var92);
var94 obj_typ_nst94:=obj_typ_nst94(var93);
var95 obj_typ_nst95:=obj_typ_nst95(var94);
var96 obj_typ_nst96:=obj_typ_nst96(var95);
var97 obj_typ_nst97:=obj_typ_nst97(var96);
var98 obj_typ_nst98:=obj_typ_nst98(var97);
var99 obj_typ_nst99:=obj_typ_nst99(var98);
var100 obj_typ_nst100:=obj_typ_nst100(var99);
var101 obj_typ_nst101:=obj_typ_nst101(var100);
var102 obj_typ_nst102:=obj_typ_nst102(var101);
var103 obj_typ_nst103:=obj_typ_nst103(var102);
var104 obj_typ_nst104:=obj_typ_nst104(var103);
var105 obj_typ_nst105:=obj_typ_nst105(var104);
var106 obj_typ_nst106:=obj_typ_nst106(var105);
var107 obj_typ_nst107:=obj_typ_nst107(var106);
var108 obj_typ_nst108:=obj_typ_nst108(var107);
var109 obj_typ_nst109:=obj_typ_nst109(var108);
var110 obj_typ_nst110:=obj_typ_nst110(var109);
var111 obj_typ_nst111:=obj_typ_nst111(var110);
var112 obj_typ_nst112:=obj_typ_nst112(var111);
var113 obj_typ_nst113:=obj_typ_nst113(var112);
var114 obj_typ_nst114:=obj_typ_nst114(var113);
var115 obj_typ_nst115:=obj_typ_nst115(var114);
var116 obj_typ_nst116:=obj_typ_nst116(var115);
var117 obj_typ_nst117:=obj_typ_nst117(var116);
var118 obj_typ_nst118:=obj_typ_nst118(var117);
var119 obj_typ_nst119:=obj_typ_nst119(var118);
var120 obj_typ_nst120:=obj_typ_nst120(var119);
var121 obj_typ_nst121:=obj_typ_nst121(var120);
var122 obj_typ_nst122:=obj_typ_nst122(var121);
var123 obj_typ_nst123:=obj_typ_nst123(var122);
var124 obj_typ_nst124:=obj_typ_nst124(var123);
var125 obj_typ_nst125:=obj_typ_nst125(var124);
var126 obj_typ_nst126:=obj_typ_nst126(var125);
var127 obj_typ_nst127:=obj_typ_nst127(var126);
var128 obj_typ_nst128:=obj_typ_nst128(var127);
var129 obj_typ_nst129:=obj_typ_nst129(var128);
var130 obj_typ_nst130:=obj_typ_nst130(var129);
var131 obj_typ_nst131:=obj_typ_nst131(var130);
var132 obj_typ_nst132:=obj_typ_nst132(var131);
var133 obj_typ_nst133:=obj_typ_nst133(var132);
var134 obj_typ_nst134:=obj_typ_nst134(var133);
var135 obj_typ_nst135:=obj_typ_nst135(var134);
var136 obj_typ_nst136:=obj_typ_nst136(var135);
var137 obj_typ_nst137:=obj_typ_nst137(var136);
var138 obj_typ_nst138:=obj_typ_nst138(var137);
var139 obj_typ_nst139:=obj_typ_nst139(var138);
var140 obj_typ_nst140:=obj_typ_nst140(var139);
var141 obj_typ_nst141:=obj_typ_nst141(var140);
var142 obj_typ_nst142:=obj_typ_nst142(var141);
var143 obj_typ_nst143:=obj_typ_nst143(var142);
var144 obj_typ_nst144:=obj_typ_nst144(var143);
var145 obj_typ_nst145:=obj_typ_nst145(var144);
var146 obj_typ_nst146:=obj_typ_nst146(var145);
var147 obj_typ_nst147:=obj_typ_nst147(var146);
var148 obj_typ_nst148:=obj_typ_nst148(var147);
var149 obj_typ_nst149:=obj_typ_nst149(var148);
var150 obj_typ_nst150:=obj_typ_nst150(var149);
var151 obj_typ_nst151:=obj_typ_nst151(var150);
var152 obj_typ_nst152:=obj_typ_nst152(var151);
var153 obj_typ_nst153:=obj_typ_nst153(var152);
var154 obj_typ_nst154:=obj_typ_nst154(var153);
var155 obj_typ_nst155:=obj_typ_nst155(var154);
var156 obj_typ_nst156:=obj_typ_nst156(var155);
var157 obj_typ_nst157:=obj_typ_nst157(var156);
var158 obj_typ_nst158:=obj_typ_nst158(var157);
var159 obj_typ_nst159:=obj_typ_nst159(var158);
var160 obj_typ_nst160:=obj_typ_nst160(var159);
var161 obj_typ_nst161:=obj_typ_nst161(var160);
var162 obj_typ_nst162:=obj_typ_nst162(var161);
var163 obj_typ_nst163:=obj_typ_nst163(var162);
var164 obj_typ_nst164:=obj_typ_nst164(var163);
var165 obj_typ_nst165:=obj_typ_nst165(var164);
var166 obj_typ_nst166:=obj_typ_nst166(var165);
var167 obj_typ_nst167:=obj_typ_nst167(var166);
var168 obj_typ_nst168:=obj_typ_nst168(var167);
var169 obj_typ_nst169:=obj_typ_nst169(var168);
var170 obj_typ_nst170:=obj_typ_nst170(var169);
var171 obj_typ_nst171:=obj_typ_nst171(var170);
var172 obj_typ_nst172:=obj_typ_nst172(var171);
var173 obj_typ_nst173:=obj_typ_nst173(var172);
var174 obj_typ_nst174:=obj_typ_nst174(var173);
var175 obj_typ_nst175:=obj_typ_nst175(var174);
var176 obj_typ_nst176:=obj_typ_nst176(var175);
var177 obj_typ_nst177:=obj_typ_nst177(var176);
var178 obj_typ_nst178:=obj_typ_nst178(var177);
var179 obj_typ_nst179:=obj_typ_nst179(var178);
var180 obj_typ_nst180:=obj_typ_nst180(var179);
var181 obj_typ_nst181:=obj_typ_nst181(var180);
var182 obj_typ_nst182:=obj_typ_nst182(var181);
var183 obj_typ_nst183:=obj_typ_nst183(var182);
var184 obj_typ_nst184:=obj_typ_nst184(var183);
var185 obj_typ_nst185:=obj_typ_nst185(var184);
var186 obj_typ_nst186:=obj_typ_nst186(var185);
var187 obj_typ_nst187:=obj_typ_nst187(var186);
var188 obj_typ_nst188:=obj_typ_nst188(var187);
var189 obj_typ_nst189:=obj_typ_nst189(var188);
var190 obj_typ_nst190:=obj_typ_nst190(var189);
var191 obj_typ_nst191:=obj_typ_nst191(var190);
var192 obj_typ_nst192:=obj_typ_nst192(var191);
var193 obj_typ_nst193:=obj_typ_nst193(var192);
var194 obj_typ_nst194:=obj_typ_nst194(var193);
var195 obj_typ_nst195:=obj_typ_nst195(var194);
var196 obj_typ_nst196:=obj_typ_nst196(var195);
var197 obj_typ_nst197:=obj_typ_nst197(var196);
var198 obj_typ_nst198:=obj_typ_nst198(var197);
var199 obj_typ_nst199:=obj_typ_nst199(var198);
var200 obj_typ_nst200:=obj_typ_nst200(var199);
begin
insert into t0(c0) values(var1.var);
insert into t0(c0) values(var2.var.var);
insert into t0(c0) values(var3.var.var.var);
insert into t0(c0) values(var4.var.var.var.var);
insert into t0(c0) values(var5.var.var.var.var.var);
insert into t0(c0) values(var6.var.var.var.var.var.var);
insert into t0(c0) values(var7.var.var.var.var.var.var.var);
insert into t0(c0) values(var8.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var9.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var10.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var11.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var12.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var13.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var14.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var15.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var16.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var17.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var18.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var19.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var20.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var21.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var22.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var23.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var24.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var25.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var26.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var27.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var28.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var29.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var30.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var31.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var32.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var33.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var34.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var35.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var36.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var37.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var38.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var39.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var40.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var41.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var42.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var43.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var44.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var45.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var46.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var47.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var48.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var49.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var50.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var51.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var52.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var53.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var54.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var55.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var56.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var57.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var58.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var59.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var60.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var61.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var62.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var63.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var64.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var65.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var66.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var67.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var68.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var69.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var70.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var71.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var72.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var73.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var74.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var75.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var76.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var77.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var78.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var79.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var80.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var81.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var82.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var83.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var84.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var85.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var86.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var87.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var88.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var89.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var90.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var91.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var92.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var93.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var94.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var95.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var96.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var97.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var98.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var99.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var100.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var101.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var102.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var103.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var104.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var105.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var106.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var107.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var108.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var109.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var110.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var111.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var112.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var113.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var114.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var115.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var116.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var117.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var118.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var119.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var120.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var121.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var122.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var123.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var124.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var125.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var126.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var127.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var128.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var129.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var130.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var131.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var132.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var133.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var134.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var135.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var136.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var137.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var138.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var139.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var140.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var141.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var142.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var143.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var144.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var145.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var146.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var147.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var148.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var149.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var150.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var151.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var152.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var153.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var154.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var155.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var156.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var157.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var158.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var159.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var160.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var161.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var162.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var163.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var164.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var165.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var166.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var167.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var168.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var169.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var170.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var171.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var172.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var173.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var174.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var175.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var176.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var177.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var178.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var179.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var180.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var181.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var182.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var183.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var184.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var185.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var186.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var187.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var188.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var189.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var190.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var191.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var192.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var193.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var194.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var195.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var196.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var197.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var198.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var199.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
insert into t0(c0) values(var200.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var.var);
end;
/
declare
v_sql varchar2(30000):='create or replace procedure p_forall_1 is type ttb is table of tb index by pls_integer; t ttb; a tb:=tb(1);procedure p0 is b tb;begin ';
begin
for i in 1..256 loop
   v_sql := v_sql ||'b:= t('||i||');forall i in b.first..b.last insert into t0 values(b(i),b(i));';
end loop;
v_sql := v_sql||' end;begin a.extend(100,1);for i in 1..256 loop t(i):=a;end loop; p0();rollback;end;';
execute immediate v_sql;
end;
/

declare
v_sql varchar2(30000):='create or replace package pkg_forall_1 is a obj0:=obj0(0,0);type ass_array is table of obj0 index by pls_integer;aa ass_array;';
begin 
for i in 1..1024 loop
v_sql :=v_sql ||'t'||i ||' tb_obj0:=tb_obj0(a);';
end loop;
v_sql :=v_sql||'procedure p1;end;';
execute immediate v_sql;
end;
/
create or replace package body pkg_forall_1 as
procedure p1 is
begin
for i in 1..1024 loop
   execute immediate 'begin pkg_forall_1.t'||i||'.extend(100,1);end;';
end loop;
forall i in t1.first..t1.last  insert into t0 values(t1(i).c0,t1(i).c1) return obj0(c0,c1)  into aa(1);
forall i in t2.first..t2.last  insert into t0 values(t2(i).c0,t2(i).c1) return obj0(c0,c1)  into aa(2);
forall i in t3.first..t3.last  insert into t0 values(t3(i).c0,t3(i).c1) return obj0(c0,c1)  into aa(3);
forall i in t4.first..t4.last  insert into t0 values(t4(i).c0,t4(i).c1) return obj0(c0,c1)  into aa(4);
forall i in t5.first..t5.last  insert into t0 values(t5(i).c0,t5(i).c1) return obj0(c0,c1)  into aa(5);
forall i in t6.first..t6.last  insert into t0 values(t6(i).c0,t6(i).c1) return obj0(c0,c1)  into aa(6);
forall i in t7.first..t7.last  insert into t0 values(t7(i).c0,t6(i).c1) return obj0(c0,c1)  into aa(7);
forall i in t8.first..t8.last  insert into t0 values(t8(i).c0,t8(i).c1) return obj0(c0,c1)  into aa(8);
forall i in t9.first..t9.last  insert into t0 values(t9(i).c0,t9(i).c1) return obj0(c0,c1)  into aa(9);
forall i in t10.first..t10.last  insert into t0 values(t10(i).c0,t10(i).c1) return obj0(c0,c1)  into aa(10);
forall i in t11.first..t11.last  insert into t0 values(t11(i).c0,t11(i).c1) return obj0(c0,c1)  into aa(11);
forall i in t12.first..t12.last  insert into t0 values(t12(i).c0,t12(i).c1) return obj0(c0,c1)  into aa(12);
forall i in t13.first..t13.last  insert into t0 values(t13(i).c0,t13(i).c1) return obj0(c0,c1)  into aa(13);
forall i in t14.first..t14.last  insert into t0 values(t14(i).c0,t14(i).c1) return obj0(c0,c1)  into aa(14);
forall i in t15.first..t15.last  insert into t0 values(t15(i).c0,t15(i).c1) return obj0(c0,c1)  into aa(15);
forall i in t16.first..t16.last  insert into t0 values(t16(i).c0,t16(i).c1) return obj0(c0,c1)  into aa(16);
forall i in t17.first..t17.last  insert into t0 values(t17(i).c0,t16(i).c1) return obj0(c0,c1)  into aa(17);
forall i in t18.first..t18.last  insert into t0 values(t18(i).c0,t18(i).c1) return obj0(c0,c1)  into aa(18);
forall i in t19.first..t19.last  insert into t0 values(t19(i).c0,t19(i).c1) return obj0(c0,c1)  into aa(19);
forall i in t20.first..t20.last  insert into t0 values(t20(i).c0,t20(i).c1) return obj0(c0,c1)  into aa(20);
forall i in t21.first..t21.last  insert into t0 values(t21(i).c0,t21(i).c1) return obj0(c0,c1)  into aa(21);
forall i in t22.first..t22.last  insert into t0 values(t22(i).c0,t22(i).c1) return obj0(c0,c1)  into aa(22);
forall i in t23.first..t23.last  insert into t0 values(t23(i).c0,t23(i).c1) return obj0(c0,c1)  into aa(23);
forall i in t24.first..t24.last  insert into t0 values(t24(i).c0,t24(i).c1) return obj0(c0,c1)  into aa(24);
forall i in t25.first..t25.last  insert into t0 values(t25(i).c0,t25(i).c1) return obj0(c0,c1)  into aa(25);
forall i in t26.first..t26.last  insert into t0 values(t26(i).c0,t26(i).c1) return obj0(c0,c1)  into aa(26);
forall i in t27.first..t27.last  insert into t0 values(t27(i).c0,t26(i).c1) return obj0(c0,c1)  into aa(27);
forall i in t28.first..t28.last  insert into t0 values(t28(i).c0,t28(i).c1) return obj0(c0,c1)  into aa(28);
forall i in t29.first..t29.last  insert into t0 values(t29(i).c0,t29(i).c1) return obj0(c0,c1)  into aa(29);
forall i in t30.first..t30.last  insert into t0 values(t30(i).c0,t30(i).c1) return obj0(c0,c1)  into aa(30);
forall i in t31.first..t31.last  insert into t0 values(t31(i).c0,t31(i).c1) return obj0(c0,c1)  into aa(31);
forall i in t32.first..t32.last  insert into t0 values(t32(i).c0,t32(i).c1) return obj0(c0,c1)  into aa(32);
forall i in t33.first..t33.last  insert into t0 values(t33(i).c0,t33(i).c1) return obj0(c0,c1)  into aa(33);
forall i in t34.first..t34.last  insert into t0 values(t34(i).c0,t34(i).c1) return obj0(c0,c1)  into aa(34);
forall i in t35.first..t35.last  insert into t0 values(t35(i).c0,t35(i).c1) return obj0(c0,c1)  into aa(35);
forall i in t36.first..t36.last  insert into t0 values(t36(i).c0,t36(i).c1) return obj0(c0,c1)  into aa(36);
forall i in t37.first..t37.last  insert into t0 values(t37(i).c0,t36(i).c1) return obj0(c0,c1)  into aa(37);
forall i in t38.first..t38.last  insert into t0 values(t38(i).c0,t38(i).c1) return obj0(c0,c1)  into aa(38);
forall i in t39.first..t39.last  insert into t0 values(t39(i).c0,t39(i).c1) return obj0(c0,c1)  into aa(39);
forall i in t40.first..t40.last  insert into t0 values(t40(i).c0,t40(i).c1) return obj0(c0,c1)  into aa(40);
forall i in t41.first..t41.last  insert into t0 values(t41(i).c0,t41(i).c1) return obj0(c0,c1)  into aa(41);
forall i in t42.first..t42.last  insert into t0 values(t42(i).c0,t42(i).c1) return obj0(c0,c1)  into aa(42);
forall i in t43.first..t43.last  insert into t0 values(t43(i).c0,t43(i).c1) return obj0(c0,c1)  into aa(43);
forall i in t44.first..t44.last  insert into t0 values(t44(i).c0,t44(i).c1) return obj0(c0,c1)  into aa(44);
forall i in t45.first..t45.last  insert into t0 values(t45(i).c0,t45(i).c1) return obj0(c0,c1)  into aa(45);
forall i in t46.first..t46.last  insert into t0 values(t46(i).c0,t46(i).c1) return obj0(c0,c1)  into aa(46);
forall i in t47.first..t47.last  insert into t0 values(t47(i).c0,t46(i).c1) return obj0(c0,c1)  into aa(47);
forall i in t48.first..t48.last  insert into t0 values(t48(i).c0,t48(i).c1) return obj0(c0,c1)  into aa(48);
forall i in t49.first..t49.last  insert into t0 values(t49(i).c0,t49(i).c1) return obj0(c0,c1)  into aa(49);
forall i in t50.first..t50.last  insert into t0 values(t50(i).c0,t50(i).c1) return obj0(c0,c1)  into aa(50);
forall i in t51.first..t51.last  insert into t0 values(t51(i).c0,t51(i).c1) return obj0(c0,c1)  into aa(51);
forall i in t52.first..t52.last  insert into t0 values(t52(i).c0,t52(i).c1) return obj0(c0,c1)  into aa(52);
forall i in t53.first..t53.last  insert into t0 values(t53(i).c0,t53(i).c1) return obj0(c0,c1)  into aa(53);
forall i in t54.first..t54.last  insert into t0 values(t54(i).c0,t54(i).c1) return obj0(c0,c1)  into aa(54);
forall i in t55.first..t55.last  insert into t0 values(t55(i).c0,t55(i).c1) return obj0(c0,c1)  into aa(55);
forall i in t56.first..t56.last  insert into t0 values(t56(i).c0,t56(i).c1) return obj0(c0,c1)  into aa(56);
forall i in t57.first..t57.last  insert into t0 values(t57(i).c0,t56(i).c1) return obj0(c0,c1)  into aa(57);
forall i in t58.first..t58.last  insert into t0 values(t58(i).c0,t58(i).c1) return obj0(c0,c1)  into aa(58);
forall i in t59.first..t59.last  insert into t0 values(t59(i).c0,t59(i).c1) return obj0(c0,c1)  into aa(59);
forall i in t60.first..t60.last  insert into t0 values(t60(i).c0,t60(i).c1) return obj0(c0,c1)  into aa(60);
forall i in t61.first..t61.last  insert into t0 values(t61(i).c0,t61(i).c1) return obj0(c0,c1)  into aa(61);
forall i in t62.first..t62.last  insert into t0 values(t62(i).c0,t62(i).c1) return obj0(c0,c1)  into aa(62);
forall i in t63.first..t63.last  insert into t0 values(t63(i).c0,t63(i).c1) return obj0(c0,c1)  into aa(63);
forall i in t64.first..t64.last  insert into t0 values(t64(i).c0,t64(i).c1) return obj0(c0,c1)  into aa(64);
forall i in t65.first..t65.last  insert into t0 values(t65(i).c0,t65(i).c1) return obj0(c0,c1)  into aa(65);
forall i in t66.first..t66.last  insert into t0 values(t66(i).c0,t66(i).c1) return obj0(c0,c1)  into aa(66);
forall i in t67.first..t67.last  insert into t0 values(t67(i).c0,t66(i).c1) return obj0(c0,c1)  into aa(67);
forall i in t68.first..t68.last  insert into t0 values(t68(i).c0,t68(i).c1) return obj0(c0,c1)  into aa(68);
forall i in t69.first..t69.last  insert into t0 values(t69(i).c0,t69(i).c1) return obj0(c0,c1)  into aa(69);
forall i in t70.first..t70.last  insert into t0 values(t70(i).c0,t70(i).c1) return obj0(c0,c1)  into aa(70);
forall i in t71.first..t71.last  insert into t0 values(t71(i).c0,t71(i).c1) return obj0(c0,c1)  into aa(71);
forall i in t72.first..t72.last  insert into t0 values(t72(i).c0,t72(i).c1) return obj0(c0,c1)  into aa(72);
forall i in t73.first..t73.last  insert into t0 values(t73(i).c0,t73(i).c1) return obj0(c0,c1)  into aa(73);
forall i in t74.first..t74.last  insert into t0 values(t74(i).c0,t74(i).c1) return obj0(c0,c1)  into aa(74);
forall i in t75.first..t75.last  insert into t0 values(t75(i).c0,t75(i).c1) return obj0(c0,c1)  into aa(75);
forall i in t76.first..t76.last  insert into t0 values(t76(i).c0,t76(i).c1) return obj0(c0,c1)  into aa(76);
forall i in t77.first..t77.last  insert into t0 values(t77(i).c0,t76(i).c1) return obj0(c0,c1)  into aa(77);
forall i in t78.first..t78.last  insert into t0 values(t78(i).c0,t78(i).c1) return obj0(c0,c1)  into aa(78);
forall i in t79.first..t79.last  insert into t0 values(t79(i).c0,t79(i).c1) return obj0(c0,c1)  into aa(79);
forall i in t80.first..t80.last  insert into t0 values(t80(i).c0,t80(i).c1) return obj0(c0,c1)  into aa(80);
forall i in t81.first..t81.last  insert into t0 values(t81(i).c0,t81(i).c1) return obj0(c0,c1)  into aa(81);
forall i in t82.first..t82.last  insert into t0 values(t82(i).c0,t82(i).c1) return obj0(c0,c1)  into aa(82);
forall i in t83.first..t83.last  insert into t0 values(t83(i).c0,t83(i).c1) return obj0(c0,c1)  into aa(83);
forall i in t84.first..t84.last  insert into t0 values(t84(i).c0,t84(i).c1) return obj0(c0,c1)  into aa(84);
forall i in t85.first..t85.last  insert into t0 values(t85(i).c0,t85(i).c1) return obj0(c0,c1)  into aa(85);
forall i in t86.first..t86.last  insert into t0 values(t86(i).c0,t86(i).c1) return obj0(c0,c1)  into aa(86);
forall i in t87.first..t87.last  insert into t0 values(t87(i).c0,t86(i).c1) return obj0(c0,c1)  into aa(87);
forall i in t88.first..t88.last  insert into t0 values(t88(i).c0,t88(i).c1) return obj0(c0,c1)  into aa(88);
forall i in t89.first..t89.last  insert into t0 values(t89(i).c0,t89(i).c1) return obj0(c0,c1)  into aa(89);
forall i in t90.first..t90.last  insert into t0 values(t90(i).c0,t90(i).c1) return obj0(c0,c1)  into aa(90);
forall i in t91.first..t91.last  insert into t0 values(t91(i).c0,t91(i).c1) return obj0(c0,c1)  into aa(91);
forall i in t92.first..t92.last  insert into t0 values(t92(i).c0,t92(i).c1) return obj0(c0,c1)  into aa(92);
forall i in t93.first..t93.last  insert into t0 values(t93(i).c0,t93(i).c1) return obj0(c0,c1)  into aa(93);
forall i in t94.first..t94.last  insert into t0 values(t94(i).c0,t94(i).c1) return obj0(c0,c1)  into aa(94);
forall i in t95.first..t95.last  insert into t0 values(t95(i).c0,t95(i).c1) return obj0(c0,c1)  into aa(95);
forall i in t96.first..t96.last  insert into t0 values(t96(i).c0,t96(i).c1) return obj0(c0,c1)  into aa(96);
forall i in t97.first..t97.last  insert into t0 values(t97(i).c0,t96(i).c1) return obj0(c0,c1)  into aa(97);
forall i in t98.first..t98.last  insert into t0 values(t98(i).c0,t98(i).c1) return obj0(c0,c1)  into aa(98);
forall i in t99.first..t99.last  insert into t0 values(t99(i).c0,t99(i).c1) return obj0(c0,c1)  into aa(99);
forall i in t100.first..t100.last  insert into t0 values(t100(i).c0,t100(i).c1) return obj0(c0,c1)  into aa(100);
forall i in t101.first..t101.last  insert into t0 values(t101(i).c0,t11(i).c1) return obj0(c0,c1)  into aa(101);
forall i in t102.first..t102.last  insert into t0 values(t102(i).c0,t12(i).c1) return obj0(c0,c1)  into aa(102);
forall i in t103.first..t103.last  insert into t0 values(t103(i).c0,t13(i).c1) return obj0(c0,c1)  into aa(103);
forall i in t104.first..t104.last  insert into t0 values(t104(i).c0,t14(i).c1) return obj0(c0,c1)  into aa(104);
forall i in t105.first..t105.last  insert into t0 values(t105(i).c0,t15(i).c1) return obj0(c0,c1)  into aa(105);
forall i in t106.first..t106.last  insert into t0 values(t106(i).c0,t16(i).c1) return obj0(c0,c1)  into aa(106);
forall i in t107.first..t107.last  insert into t0 values(t107(i).c0,t16(i).c1) return obj0(c0,c1)  into aa(107);
forall i in t108.first..t108.last  insert into t0 values(t108(i).c0,t18(i).c1) return obj0(c0,c1)  into aa(108);
forall i in t109.first..t109.last  insert into t0 values(t109(i).c0,t19(i).c1) return obj0(c0,c1)  into aa(109);
forall i in t110.first..t110.last  insert into t0 values(t110(i).c0,t110(i).c1) return obj0(c0,c1)  into aa(110);
forall i in t111.first..t111.last  insert into t0 values(t111(i).c0,t111(i).c1) return obj0(c0,c1)  into aa(111);
forall i in t112.first..t112.last  insert into t0 values(t112(i).c0,t112(i).c1) return obj0(c0,c1)  into aa(112);
forall i in t113.first..t113.last  insert into t0 values(t113(i).c0,t113(i).c1) return obj0(c0,c1)  into aa(113);
forall i in t114.first..t114.last  insert into t0 values(t114(i).c0,t114(i).c1) return obj0(c0,c1)  into aa(114);
forall i in t115.first..t115.last  insert into t0 values(t115(i).c0,t115(i).c1) return obj0(c0,c1)  into aa(115);
forall i in t116.first..t116.last  insert into t0 values(t116(i).c0,t116(i).c1) return obj0(c0,c1)  into aa(116);
forall i in t117.first..t117.last  insert into t0 values(t117(i).c0,t116(i).c1) return obj0(c0,c1)  into aa(117);
forall i in t118.first..t118.last  insert into t0 values(t118(i).c0,t118(i).c1) return obj0(c0,c1)  into aa(118);
forall i in t119.first..t119.last  insert into t0 values(t119(i).c0,t119(i).c1) return obj0(c0,c1)  into aa(119);
forall i in t120.first..t120.last  insert into t0 values(t120(i).c0,t120(i).c1) return obj0(c0,c1)  into aa(120);
forall i in t121.first..t121.last  insert into t0 values(t121(i).c0,t121(i).c1) return obj0(c0,c1)  into aa(121);
forall i in t122.first..t122.last  insert into t0 values(t122(i).c0,t122(i).c1) return obj0(c0,c1)  into aa(122);
forall i in t123.first..t123.last  insert into t0 values(t123(i).c0,t123(i).c1) return obj0(c0,c1)  into aa(123);
forall i in t124.first..t124.last  insert into t0 values(t124(i).c0,t124(i).c1) return obj0(c0,c1)  into aa(124);
forall i in t125.first..t125.last  insert into t0 values(t125(i).c0,t125(i).c1) return obj0(c0,c1)  into aa(125);
forall i in t126.first..t126.last  insert into t0 values(t126(i).c0,t126(i).c1) return obj0(c0,c1)  into aa(126);
forall i in t127.first..t127.last  insert into t0 values(t127(i).c0,t126(i).c1) return obj0(c0,c1)  into aa(127);
forall i in t128.first..t128.last  insert into t0 values(t128(i).c0,t128(i).c1) return obj0(c0,c1)  into aa(128);
forall i in t129.first..t129.last  insert into t0 values(t129(i).c0,t129(i).c1) return obj0(c0,c1)  into aa(129);
forall i in t130.first..t130.last  insert into t0 values(t130(i).c0,t130(i).c1) return obj0(c0,c1)  into aa(130);
forall i in t131.first..t131.last  insert into t0 values(t131(i).c0,t131(i).c1) return obj0(c0,c1)  into aa(131);
forall i in t132.first..t132.last  insert into t0 values(t132(i).c0,t132(i).c1) return obj0(c0,c1)  into aa(132);
forall i in t133.first..t133.last  insert into t0 values(t133(i).c0,t133(i).c1) return obj0(c0,c1)  into aa(133);
forall i in t134.first..t134.last  insert into t0 values(t134(i).c0,t134(i).c1) return obj0(c0,c1)  into aa(134);
forall i in t135.first..t135.last  insert into t0 values(t135(i).c0,t135(i).c1) return obj0(c0,c1)  into aa(135);
forall i in t136.first..t136.last  insert into t0 values(t136(i).c0,t136(i).c1) return obj0(c0,c1)  into aa(136);
forall i in t137.first..t137.last  insert into t0 values(t137(i).c0,t136(i).c1) return obj0(c0,c1)  into aa(137);
forall i in t138.first..t138.last  insert into t0 values(t138(i).c0,t138(i).c1) return obj0(c0,c1)  into aa(138);
forall i in t139.first..t139.last  insert into t0 values(t139(i).c0,t139(i).c1) return obj0(c0,c1)  into aa(139);
forall i in t140.first..t140.last  insert into t0 values(t140(i).c0,t140(i).c1) return obj0(c0,c1)  into aa(140);
forall i in t141.first..t141.last  insert into t0 values(t141(i).c0,t141(i).c1) return obj0(c0,c1)  into aa(141);
forall i in t142.first..t142.last  insert into t0 values(t142(i).c0,t142(i).c1) return obj0(c0,c1)  into aa(142);
forall i in t143.first..t143.last  insert into t0 values(t143(i).c0,t143(i).c1) return obj0(c0,c1)  into aa(143);
forall i in t144.first..t144.last  insert into t0 values(t144(i).c0,t144(i).c1) return obj0(c0,c1)  into aa(144);
forall i in t145.first..t145.last  insert into t0 values(t145(i).c0,t145(i).c1) return obj0(c0,c1)  into aa(145);
forall i in t146.first..t146.last  insert into t0 values(t146(i).c0,t146(i).c1) return obj0(c0,c1)  into aa(146);
forall i in t147.first..t147.last  insert into t0 values(t147(i).c0,t146(i).c1) return obj0(c0,c1)  into aa(147);
forall i in t148.first..t148.last  insert into t0 values(t148(i).c0,t148(i).c1) return obj0(c0,c1)  into aa(148);
forall i in t149.first..t149.last  insert into t0 values(t149(i).c0,t149(i).c1) return obj0(c0,c1)  into aa(149);
forall i in t150.first..t150.last  insert into t0 values(t150(i).c0,t150(i).c1) return obj0(c0,c1)  into aa(150);
forall i in t151.first..t151.last  insert into t0 values(t151(i).c0,t151(i).c1) return obj0(c0,c1)  into aa(151);
forall i in t152.first..t152.last  insert into t0 values(t152(i).c0,t152(i).c1) return obj0(c0,c1)  into aa(152);
forall i in t153.first..t153.last  insert into t0 values(t153(i).c0,t153(i).c1) return obj0(c0,c1)  into aa(153);
forall i in t154.first..t154.last  insert into t0 values(t154(i).c0,t154(i).c1) return obj0(c0,c1)  into aa(154);
forall i in t155.first..t155.last  insert into t0 values(t155(i).c0,t155(i).c1) return obj0(c0,c1)  into aa(155);
forall i in t156.first..t156.last  insert into t0 values(t156(i).c0,t156(i).c1) return obj0(c0,c1)  into aa(156);
forall i in t157.first..t157.last  insert into t0 values(t157(i).c0,t156(i).c1) return obj0(c0,c1)  into aa(157);
forall i in t158.first..t158.last  insert into t0 values(t158(i).c0,t158(i).c1) return obj0(c0,c1)  into aa(158);
forall i in t159.first..t159.last  insert into t0 values(t159(i).c0,t159(i).c1) return obj0(c0,c1)  into aa(159);
forall i in t160.first..t160.last  insert into t0 values(t160(i).c0,t160(i).c1) return obj0(c0,c1)  into aa(160);
forall i in t161.first..t161.last  insert into t0 values(t161(i).c0,t161(i).c1) return obj0(c0,c1)  into aa(161);
forall i in t162.first..t162.last  insert into t0 values(t162(i).c0,t162(i).c1) return obj0(c0,c1)  into aa(162);
forall i in t163.first..t163.last  insert into t0 values(t163(i).c0,t163(i).c1) return obj0(c0,c1)  into aa(163);
forall i in t164.first..t164.last  insert into t0 values(t164(i).c0,t164(i).c1) return obj0(c0,c1)  into aa(164);
forall i in t165.first..t165.last  insert into t0 values(t165(i).c0,t165(i).c1) return obj0(c0,c1)  into aa(165);
forall i in t166.first..t166.last  insert into t0 values(t166(i).c0,t166(i).c1) return obj0(c0,c1)  into aa(166);
forall i in t167.first..t167.last  insert into t0 values(t167(i).c0,t166(i).c1) return obj0(c0,c1)  into aa(167);
forall i in t168.first..t168.last  insert into t0 values(t168(i).c0,t168(i).c1) return obj0(c0,c1)  into aa(168);
forall i in t169.first..t169.last  insert into t0 values(t169(i).c0,t169(i).c1) return obj0(c0,c1)  into aa(169);
forall i in t170.first..t170.last  insert into t0 values(t170(i).c0,t170(i).c1) return obj0(c0,c1)  into aa(170);
forall i in t171.first..t171.last  insert into t0 values(t171(i).c0,t171(i).c1) return obj0(c0,c1)  into aa(171);
forall i in t172.first..t172.last  insert into t0 values(t172(i).c0,t172(i).c1) return obj0(c0,c1)  into aa(172);
forall i in t173.first..t173.last  insert into t0 values(t173(i).c0,t173(i).c1) return obj0(c0,c1)  into aa(173);
forall i in t174.first..t174.last  insert into t0 values(t174(i).c0,t174(i).c1) return obj0(c0,c1)  into aa(174);
forall i in t175.first..t175.last  insert into t0 values(t175(i).c0,t175(i).c1) return obj0(c0,c1)  into aa(175);
forall i in t176.first..t176.last  insert into t0 values(t176(i).c0,t176(i).c1) return obj0(c0,c1)  into aa(176);
forall i in t177.first..t177.last  insert into t0 values(t177(i).c0,t176(i).c1) return obj0(c0,c1)  into aa(177);
forall i in t178.first..t178.last  insert into t0 values(t178(i).c0,t178(i).c1) return obj0(c0,c1)  into aa(178);
forall i in t179.first..t179.last  insert into t0 values(t179(i).c0,t179(i).c1) return obj0(c0,c1)  into aa(179);
forall i in t180.first..t180.last  insert into t0 values(t180(i).c0,t180(i).c1) return obj0(c0,c1)  into aa(180);
forall i in t181.first..t181.last  insert into t0 values(t181(i).c0,t181(i).c1) return obj0(c0,c1)  into aa(181);
forall i in t182.first..t182.last  insert into t0 values(t182(i).c0,t182(i).c1) return obj0(c0,c1)  into aa(182);
forall i in t183.first..t183.last  insert into t0 values(t183(i).c0,t183(i).c1) return obj0(c0,c1)  into aa(183);
forall i in t184.first..t184.last  insert into t0 values(t184(i).c0,t184(i).c1) return obj0(c0,c1)  into aa(184);
forall i in t185.first..t185.last  insert into t0 values(t185(i).c0,t185(i).c1) return obj0(c0,c1)  into aa(185);
forall i in t186.first..t186.last  insert into t0 values(t186(i).c0,t186(i).c1) return obj0(c0,c1)  into aa(186);
forall i in t187.first..t187.last  insert into t0 values(t187(i).c0,t186(i).c1) return obj0(c0,c1)  into aa(187);
forall i in t188.first..t188.last  insert into t0 values(t188(i).c0,t188(i).c1) return obj0(c0,c1)  into aa(188);
forall i in t189.first..t189.last  insert into t0 values(t189(i).c0,t189(i).c1) return obj0(c0,c1)  into aa(189);
forall i in t190.first..t190.last  insert into t0 values(t190(i).c0,t190(i).c1) return obj0(c0,c1)  into aa(190);
forall i in t191.first..t191.last  insert into t0 values(t191(i).c0,t191(i).c1) return obj0(c0,c1)  into aa(191);
forall i in t192.first..t192.last  insert into t0 values(t192(i).c0,t192(i).c1) return obj0(c0,c1)  into aa(192);
forall i in t193.first..t193.last  insert into t0 values(t193(i).c0,t193(i).c1) return obj0(c0,c1)  into aa(193);
forall i in t194.first..t194.last  insert into t0 values(t194(i).c0,t194(i).c1) return obj0(c0,c1)  into aa(194);
forall i in t195.first..t195.last  insert into t0 values(t195(i).c0,t195(i).c1) return obj0(c0,c1)  into aa(195);
forall i in t196.first..t196.last  insert into t0 values(t196(i).c0,t196(i).c1) return obj0(c0,c1)  into aa(196);
forall i in t197.first..t197.last  insert into t0 values(t197(i).c0,t196(i).c1) return obj0(c0,c1)  into aa(197);
forall i in t198.first..t198.last  insert into t0 values(t198(i).c0,t198(i).c1) return obj0(c0,c1)  into aa(198);
forall i in t199.first..t199.last  insert into t0 values(t199(i).c0,t199(i).c1) return obj0(c0,c1)  into aa(199);
forall i in t200.first..t200.last  insert into t0 values(t200(i).c0,t200(i).c1) return obj0(c0,c1)  into aa(200);
forall i in t201.first..t201.last  insert into t0 values(t201(i).c0,t21(i).c1) return obj0(c0,c1)  into aa(201);
forall i in t202.first..t202.last  insert into t0 values(t202(i).c0,t22(i).c1) return obj0(c0,c1)  into aa(202);
forall i in t203.first..t203.last  insert into t0 values(t203(i).c0,t23(i).c1) return obj0(c0,c1)  into aa(203);
forall i in t204.first..t204.last  insert into t0 values(t204(i).c0,t24(i).c1) return obj0(c0,c1)  into aa(204);
forall i in t205.first..t205.last  insert into t0 values(t205(i).c0,t25(i).c1) return obj0(c0,c1)  into aa(205);
forall i in t206.first..t206.last  insert into t0 values(t206(i).c0,t26(i).c1) return obj0(c0,c1)  into aa(206);
forall i in t207.first..t207.last  insert into t0 values(t207(i).c0,t26(i).c1) return obj0(c0,c1)  into aa(207);
forall i in t208.first..t208.last  insert into t0 values(t208(i).c0,t28(i).c1) return obj0(c0,c1)  into aa(208);
forall i in t209.first..t209.last  insert into t0 values(t209(i).c0,t29(i).c1) return obj0(c0,c1)  into aa(209);
forall i in t210.first..t210.last  insert into t0 values(t210(i).c0,t210(i).c1) return obj0(c0,c1)  into aa(210);
forall i in t211.first..t211.last  insert into t0 values(t211(i).c0,t211(i).c1) return obj0(c0,c1)  into aa(211);
forall i in t212.first..t212.last  insert into t0 values(t212(i).c0,t212(i).c1) return obj0(c0,c1)  into aa(212);
forall i in t213.first..t213.last  insert into t0 values(t213(i).c0,t213(i).c1) return obj0(c0,c1)  into aa(213);
forall i in t214.first..t214.last  insert into t0 values(t214(i).c0,t214(i).c1) return obj0(c0,c1)  into aa(214);
forall i in t215.first..t215.last  insert into t0 values(t215(i).c0,t215(i).c1) return obj0(c0,c1)  into aa(215);
forall i in t216.first..t216.last  insert into t0 values(t216(i).c0,t216(i).c1) return obj0(c0,c1)  into aa(216);
forall i in t217.first..t217.last  insert into t0 values(t217(i).c0,t216(i).c1) return obj0(c0,c1)  into aa(217);
forall i in t218.first..t218.last  insert into t0 values(t218(i).c0,t218(i).c1) return obj0(c0,c1)  into aa(218);
forall i in t219.first..t219.last  insert into t0 values(t219(i).c0,t219(i).c1) return obj0(c0,c1)  into aa(219);
forall i in t220.first..t220.last  insert into t0 values(t220(i).c0,t220(i).c1) return obj0(c0,c1)  into aa(220);
forall i in t221.first..t221.last  insert into t0 values(t221(i).c0,t221(i).c1) return obj0(c0,c1)  into aa(221);
forall i in t222.first..t222.last  insert into t0 values(t222(i).c0,t222(i).c1) return obj0(c0,c1)  into aa(222);
forall i in t223.first..t223.last  insert into t0 values(t223(i).c0,t223(i).c1) return obj0(c0,c1)  into aa(223);
forall i in t224.first..t224.last  insert into t0 values(t224(i).c0,t224(i).c1) return obj0(c0,c1)  into aa(224);
forall i in t225.first..t225.last  insert into t0 values(t225(i).c0,t225(i).c1) return obj0(c0,c1)  into aa(225);
forall i in t226.first..t226.last  insert into t0 values(t226(i).c0,t226(i).c1) return obj0(c0,c1)  into aa(226);
forall i in t227.first..t227.last  insert into t0 values(t227(i).c0,t226(i).c1) return obj0(c0,c1)  into aa(227);
forall i in t228.first..t228.last  insert into t0 values(t228(i).c0,t228(i).c1) return obj0(c0,c1)  into aa(228);
forall i in t229.first..t229.last  insert into t0 values(t229(i).c0,t229(i).c1) return obj0(c0,c1)  into aa(229);
forall i in t230.first..t230.last  insert into t0 values(t230(i).c0,t230(i).c1) return obj0(c0,c1)  into aa(230);
forall i in t231.first..t231.last  insert into t0 values(t231(i).c0,t231(i).c1) return obj0(c0,c1)  into aa(231);
forall i in t232.first..t232.last  insert into t0 values(t232(i).c0,t232(i).c1) return obj0(c0,c1)  into aa(232);
forall i in t233.first..t233.last  insert into t0 values(t233(i).c0,t233(i).c1) return obj0(c0,c1)  into aa(233);
forall i in t234.first..t234.last  insert into t0 values(t234(i).c0,t234(i).c1) return obj0(c0,c1)  into aa(234);
forall i in t235.first..t235.last  insert into t0 values(t235(i).c0,t235(i).c1) return obj0(c0,c1)  into aa(235);
forall i in t236.first..t236.last  insert into t0 values(t236(i).c0,t236(i).c1) return obj0(c0,c1)  into aa(236);
forall i in t237.first..t237.last  insert into t0 values(t237(i).c0,t236(i).c1) return obj0(c0,c1)  into aa(237);
forall i in t238.first..t238.last  insert into t0 values(t238(i).c0,t238(i).c1) return obj0(c0,c1)  into aa(238);
forall i in t239.first..t239.last  insert into t0 values(t239(i).c0,t239(i).c1) return obj0(c0,c1)  into aa(239);
forall i in t240.first..t240.last  insert into t0 values(t240(i).c0,t240(i).c1) return obj0(c0,c1)  into aa(240);
forall i in t241.first..t241.last  insert into t0 values(t241(i).c0,t241(i).c1) return obj0(c0,c1)  into aa(241);
forall i in t242.first..t242.last  insert into t0 values(t242(i).c0,t242(i).c1) return obj0(c0,c1)  into aa(242);
forall i in t243.first..t243.last  insert into t0 values(t243(i).c0,t243(i).c1) return obj0(c0,c1)  into aa(243);
forall i in t244.first..t244.last  insert into t0 values(t244(i).c0,t244(i).c1) return obj0(c0,c1)  into aa(244);
forall i in t245.first..t245.last  insert into t0 values(t245(i).c0,t245(i).c1) return obj0(c0,c1)  into aa(245);
forall i in t246.first..t246.last  insert into t0 values(t246(i).c0,t246(i).c1) return obj0(c0,c1)  into aa(246);
forall i in t247.first..t247.last  insert into t0 values(t247(i).c0,t246(i).c1) return obj0(c0,c1)  into aa(247);
forall i in t248.first..t248.last  insert into t0 values(t248(i).c0,t248(i).c1) return obj0(c0,c1)  into aa(248);
forall i in t249.first..t249.last  insert into t0 values(t249(i).c0,t249(i).c1) return obj0(c0,c1)  into aa(249);
forall i in t250.first..t250.last  insert into t0 values(t250(i).c0,t250(i).c1) return obj0(c0,c1)  into aa(250);
forall i in t251.first..t251.last  insert into t0 values(t251(i).c0,t251(i).c1) return obj0(c0,c1)  into aa(251);
forall i in t252.first..t252.last  insert into t0 values(t252(i).c0,t252(i).c1) return obj0(c0,c1)  into aa(252);
forall i in t253.first..t253.last  insert into t0 values(t253(i).c0,t253(i).c1) return obj0(c0,c1)  into aa(253);
forall i in t254.first..t254.last  insert into t0 values(t254(i).c0,t254(i).c1) return obj0(c0,c1)  into aa(254);
forall i in t255.first..t255.last  insert into t0 values(t255(i).c0,t255(i).c1) return obj0(c0,c1)  into aa(255);
forall i in t256.first..t256.last  insert into t0 values(t256(i).c0,t256(i).c1) return obj0(c0,c1)  into aa(256);
forall i in t257.first..t257.last  insert into t0 values(t257(i).c0,t256(i).c1) return obj0(c0,c1)  into aa(257);
forall i in t258.first..t258.last  insert into t0 values(t258(i).c0,t258(i).c1) return obj0(c0,c1)  into aa(258);
forall i in t259.first..t259.last  insert into t0 values(t259(i).c0,t259(i).c1) return obj0(c0,c1)  into aa(259);
forall i in t260.first..t260.last  insert into t0 values(t260(i).c0,t260(i).c1) return obj0(c0,c1)  into aa(260);
forall i in t261.first..t261.last  insert into t0 values(t261(i).c0,t261(i).c1) return obj0(c0,c1)  into aa(261);
forall i in t262.first..t262.last  insert into t0 values(t262(i).c0,t262(i).c1) return obj0(c0,c1)  into aa(262);
forall i in t263.first..t263.last  insert into t0 values(t263(i).c0,t263(i).c1) return obj0(c0,c1)  into aa(263);
forall i in t264.first..t264.last  insert into t0 values(t264(i).c0,t264(i).c1) return obj0(c0,c1)  into aa(264);
forall i in t265.first..t265.last  insert into t0 values(t265(i).c0,t265(i).c1) return obj0(c0,c1)  into aa(265);
forall i in t266.first..t266.last  insert into t0 values(t266(i).c0,t266(i).c1) return obj0(c0,c1)  into aa(266);
forall i in t267.first..t267.last  insert into t0 values(t267(i).c0,t266(i).c1) return obj0(c0,c1)  into aa(267);
forall i in t268.first..t268.last  insert into t0 values(t268(i).c0,t268(i).c1) return obj0(c0,c1)  into aa(268);
forall i in t269.first..t269.last  insert into t0 values(t269(i).c0,t269(i).c1) return obj0(c0,c1)  into aa(269);
forall i in t270.first..t270.last  insert into t0 values(t270(i).c0,t270(i).c1) return obj0(c0,c1)  into aa(270);
forall i in t271.first..t271.last  insert into t0 values(t271(i).c0,t271(i).c1) return obj0(c0,c1)  into aa(271);
forall i in t272.first..t272.last  insert into t0 values(t272(i).c0,t272(i).c1) return obj0(c0,c1)  into aa(272);
forall i in t273.first..t273.last  insert into t0 values(t273(i).c0,t273(i).c1) return obj0(c0,c1)  into aa(273);
forall i in t274.first..t274.last  insert into t0 values(t274(i).c0,t274(i).c1) return obj0(c0,c1)  into aa(274);
forall i in t275.first..t275.last  insert into t0 values(t275(i).c0,t275(i).c1) return obj0(c0,c1)  into aa(275);
forall i in t276.first..t276.last  insert into t0 values(t276(i).c0,t276(i).c1) return obj0(c0,c1)  into aa(276);
forall i in t277.first..t277.last  insert into t0 values(t277(i).c0,t276(i).c1) return obj0(c0,c1)  into aa(277);
forall i in t278.first..t278.last  insert into t0 values(t278(i).c0,t278(i).c1) return obj0(c0,c1)  into aa(278);
forall i in t279.first..t279.last  insert into t0 values(t279(i).c0,t279(i).c1) return obj0(c0,c1)  into aa(279);
forall i in t280.first..t280.last  insert into t0 values(t280(i).c0,t280(i).c1) return obj0(c0,c1)  into aa(280);
forall i in t281.first..t281.last  insert into t0 values(t281(i).c0,t281(i).c1) return obj0(c0,c1)  into aa(281);
forall i in t282.first..t282.last  insert into t0 values(t282(i).c0,t282(i).c1) return obj0(c0,c1)  into aa(282);
forall i in t283.first..t283.last  insert into t0 values(t283(i).c0,t283(i).c1) return obj0(c0,c1)  into aa(283);
forall i in t284.first..t284.last  insert into t0 values(t284(i).c0,t284(i).c1) return obj0(c0,c1)  into aa(284);
forall i in t285.first..t285.last  insert into t0 values(t285(i).c0,t285(i).c1) return obj0(c0,c1)  into aa(285);
forall i in t286.first..t286.last  insert into t0 values(t286(i).c0,t286(i).c1) return obj0(c0,c1)  into aa(286);
forall i in t287.first..t287.last  insert into t0 values(t287(i).c0,t286(i).c1) return obj0(c0,c1)  into aa(287);
forall i in t288.first..t288.last  insert into t0 values(t288(i).c0,t288(i).c1) return obj0(c0,c1)  into aa(288);
forall i in t289.first..t289.last  insert into t0 values(t289(i).c0,t289(i).c1) return obj0(c0,c1)  into aa(289);
forall i in t290.first..t290.last  insert into t0 values(t290(i).c0,t290(i).c1) return obj0(c0,c1)  into aa(290);
forall i in t291.first..t291.last  insert into t0 values(t291(i).c0,t291(i).c1) return obj0(c0,c1)  into aa(291);
forall i in t292.first..t292.last  insert into t0 values(t292(i).c0,t292(i).c1) return obj0(c0,c1)  into aa(292);
forall i in t293.first..t293.last  insert into t0 values(t293(i).c0,t293(i).c1) return obj0(c0,c1)  into aa(293);
forall i in t294.first..t294.last  insert into t0 values(t294(i).c0,t294(i).c1) return obj0(c0,c1)  into aa(294);
forall i in t295.first..t295.last  insert into t0 values(t295(i).c0,t295(i).c1) return obj0(c0,c1)  into aa(295);
forall i in t296.first..t296.last  insert into t0 values(t296(i).c0,t296(i).c1) return obj0(c0,c1)  into aa(296);
forall i in t297.first..t297.last  insert into t0 values(t297(i).c0,t296(i).c1) return obj0(c0,c1)  into aa(297);
forall i in t298.first..t298.last  insert into t0 values(t298(i).c0,t298(i).c1) return obj0(c0,c1)  into aa(298);
forall i in t299.first..t299.last  insert into t0 values(t299(i).c0,t299(i).c1) return obj0(c0,c1)  into aa(299);
forall i in t300.first..t300.last  insert into t0 values(t300(i).c0,t300(i).c1) return obj0(c0,c1)  into aa(300);
forall i in t301.first..t301.last  insert into t0 values(t301(i).c0,t31(i).c1) return obj0(c0,c1)  into aa(301);
forall i in t302.first..t302.last  insert into t0 values(t302(i).c0,t32(i).c1) return obj0(c0,c1)  into aa(302);
forall i in t303.first..t303.last  insert into t0 values(t303(i).c0,t33(i).c1) return obj0(c0,c1)  into aa(303);
forall i in t304.first..t304.last  insert into t0 values(t304(i).c0,t34(i).c1) return obj0(c0,c1)  into aa(304);
forall i in t305.first..t305.last  insert into t0 values(t305(i).c0,t35(i).c1) return obj0(c0,c1)  into aa(305);
forall i in t306.first..t306.last  insert into t0 values(t306(i).c0,t36(i).c1) return obj0(c0,c1)  into aa(306);
forall i in t307.first..t307.last  insert into t0 values(t307(i).c0,t36(i).c1) return obj0(c0,c1)  into aa(307);
forall i in t308.first..t308.last  insert into t0 values(t308(i).c0,t38(i).c1) return obj0(c0,c1)  into aa(308);
forall i in t309.first..t309.last  insert into t0 values(t309(i).c0,t39(i).c1) return obj0(c0,c1)  into aa(309);
forall i in t310.first..t310.last  insert into t0 values(t310(i).c0,t310(i).c1) return obj0(c0,c1)  into aa(310);
forall i in t311.first..t311.last  insert into t0 values(t311(i).c0,t311(i).c1) return obj0(c0,c1)  into aa(311);
forall i in t312.first..t312.last  insert into t0 values(t312(i).c0,t312(i).c1) return obj0(c0,c1)  into aa(312);
forall i in t313.first..t313.last  insert into t0 values(t313(i).c0,t313(i).c1) return obj0(c0,c1)  into aa(313);
forall i in t314.first..t314.last  insert into t0 values(t314(i).c0,t314(i).c1) return obj0(c0,c1)  into aa(314);
forall i in t315.first..t315.last  insert into t0 values(t315(i).c0,t315(i).c1) return obj0(c0,c1)  into aa(315);
forall i in t316.first..t316.last  insert into t0 values(t316(i).c0,t316(i).c1) return obj0(c0,c1)  into aa(316);
forall i in t317.first..t317.last  insert into t0 values(t317(i).c0,t316(i).c1) return obj0(c0,c1)  into aa(317);
forall i in t318.first..t318.last  insert into t0 values(t318(i).c0,t318(i).c1) return obj0(c0,c1)  into aa(318);
forall i in t319.first..t319.last  insert into t0 values(t319(i).c0,t319(i).c1) return obj0(c0,c1)  into aa(319);
forall i in t320.first..t320.last  insert into t0 values(t320(i).c0,t320(i).c1) return obj0(c0,c1)  into aa(320);
forall i in t321.first..t321.last  insert into t0 values(t321(i).c0,t321(i).c1) return obj0(c0,c1)  into aa(321);
forall i in t322.first..t322.last  insert into t0 values(t322(i).c0,t322(i).c1) return obj0(c0,c1)  into aa(322);
forall i in t323.first..t323.last  insert into t0 values(t323(i).c0,t323(i).c1) return obj0(c0,c1)  into aa(323);
forall i in t324.first..t324.last  insert into t0 values(t324(i).c0,t324(i).c1) return obj0(c0,c1)  into aa(324);
forall i in t325.first..t325.last  insert into t0 values(t325(i).c0,t325(i).c1) return obj0(c0,c1)  into aa(325);
forall i in t326.first..t326.last  insert into t0 values(t326(i).c0,t326(i).c1) return obj0(c0,c1)  into aa(326);
forall i in t327.first..t327.last  insert into t0 values(t327(i).c0,t326(i).c1) return obj0(c0,c1)  into aa(327);
forall i in t328.first..t328.last  insert into t0 values(t328(i).c0,t328(i).c1) return obj0(c0,c1)  into aa(328);
forall i in t329.first..t329.last  insert into t0 values(t329(i).c0,t329(i).c1) return obj0(c0,c1)  into aa(329);
forall i in t330.first..t330.last  insert into t0 values(t330(i).c0,t330(i).c1) return obj0(c0,c1)  into aa(330);
forall i in t331.first..t331.last  insert into t0 values(t331(i).c0,t331(i).c1) return obj0(c0,c1)  into aa(331);
forall i in t332.first..t332.last  insert into t0 values(t332(i).c0,t332(i).c1) return obj0(c0,c1)  into aa(332);
forall i in t333.first..t333.last  insert into t0 values(t333(i).c0,t333(i).c1) return obj0(c0,c1)  into aa(333);
forall i in t334.first..t334.last  insert into t0 values(t334(i).c0,t334(i).c1) return obj0(c0,c1)  into aa(334);
forall i in t335.first..t335.last  insert into t0 values(t335(i).c0,t335(i).c1) return obj0(c0,c1)  into aa(335);
forall i in t336.first..t336.last  insert into t0 values(t336(i).c0,t336(i).c1) return obj0(c0,c1)  into aa(336);
forall i in t337.first..t337.last  insert into t0 values(t337(i).c0,t336(i).c1) return obj0(c0,c1)  into aa(337);
forall i in t338.first..t338.last  insert into t0 values(t338(i).c0,t338(i).c1) return obj0(c0,c1)  into aa(338);
forall i in t339.first..t339.last  insert into t0 values(t339(i).c0,t339(i).c1) return obj0(c0,c1)  into aa(339);
forall i in t340.first..t340.last  insert into t0 values(t340(i).c0,t340(i).c1) return obj0(c0,c1)  into aa(340);
forall i in t341.first..t341.last  insert into t0 values(t341(i).c0,t341(i).c1) return obj0(c0,c1)  into aa(341);
forall i in t342.first..t342.last  insert into t0 values(t342(i).c0,t342(i).c1) return obj0(c0,c1)  into aa(342);
forall i in t343.first..t343.last  insert into t0 values(t343(i).c0,t343(i).c1) return obj0(c0,c1)  into aa(343);
forall i in t344.first..t344.last  insert into t0 values(t344(i).c0,t344(i).c1) return obj0(c0,c1)  into aa(344);
forall i in t345.first..t345.last  insert into t0 values(t345(i).c0,t345(i).c1) return obj0(c0,c1)  into aa(345);
forall i in t346.first..t346.last  insert into t0 values(t346(i).c0,t346(i).c1) return obj0(c0,c1)  into aa(346);
forall i in t347.first..t347.last  insert into t0 values(t347(i).c0,t346(i).c1) return obj0(c0,c1)  into aa(347);
forall i in t348.first..t348.last  insert into t0 values(t348(i).c0,t348(i).c1) return obj0(c0,c1)  into aa(348);
forall i in t349.first..t349.last  insert into t0 values(t349(i).c0,t349(i).c1) return obj0(c0,c1)  into aa(349);
forall i in t350.first..t350.last  insert into t0 values(t350(i).c0,t350(i).c1) return obj0(c0,c1)  into aa(350);
forall i in t351.first..t351.last  insert into t0 values(t351(i).c0,t351(i).c1) return obj0(c0,c1)  into aa(351);
forall i in t352.first..t352.last  insert into t0 values(t352(i).c0,t352(i).c1) return obj0(c0,c1)  into aa(352);
forall i in t353.first..t353.last  insert into t0 values(t353(i).c0,t353(i).c1) return obj0(c0,c1)  into aa(353);
forall i in t354.first..t354.last  insert into t0 values(t354(i).c0,t354(i).c1) return obj0(c0,c1)  into aa(354);
forall i in t355.first..t355.last  insert into t0 values(t355(i).c0,t355(i).c1) return obj0(c0,c1)  into aa(355);
forall i in t356.first..t356.last  insert into t0 values(t356(i).c0,t356(i).c1) return obj0(c0,c1)  into aa(356);
forall i in t357.first..t357.last  insert into t0 values(t357(i).c0,t356(i).c1) return obj0(c0,c1)  into aa(357);
forall i in t358.first..t358.last  insert into t0 values(t358(i).c0,t358(i).c1) return obj0(c0,c1)  into aa(358);
forall i in t359.first..t359.last  insert into t0 values(t359(i).c0,t359(i).c1) return obj0(c0,c1)  into aa(359);
forall i in t360.first..t360.last  insert into t0 values(t360(i).c0,t360(i).c1) return obj0(c0,c1)  into aa(360);
forall i in t361.first..t361.last  insert into t0 values(t361(i).c0,t361(i).c1) return obj0(c0,c1)  into aa(361);
forall i in t362.first..t362.last  insert into t0 values(t362(i).c0,t362(i).c1) return obj0(c0,c1)  into aa(362);
forall i in t363.first..t363.last  insert into t0 values(t363(i).c0,t363(i).c1) return obj0(c0,c1)  into aa(363);
forall i in t364.first..t364.last  insert into t0 values(t364(i).c0,t364(i).c1) return obj0(c0,c1)  into aa(364);
forall i in t365.first..t365.last  insert into t0 values(t365(i).c0,t365(i).c1) return obj0(c0,c1)  into aa(365);
forall i in t366.first..t366.last  insert into t0 values(t366(i).c0,t366(i).c1) return obj0(c0,c1)  into aa(366);
forall i in t367.first..t367.last  insert into t0 values(t367(i).c0,t366(i).c1) return obj0(c0,c1)  into aa(367);
forall i in t368.first..t368.last  insert into t0 values(t368(i).c0,t368(i).c1) return obj0(c0,c1)  into aa(368);
forall i in t369.first..t369.last  insert into t0 values(t369(i).c0,t369(i).c1) return obj0(c0,c1)  into aa(369);
forall i in t370.first..t370.last  insert into t0 values(t370(i).c0,t370(i).c1) return obj0(c0,c1)  into aa(370);
forall i in t371.first..t371.last  insert into t0 values(t371(i).c0,t371(i).c1) return obj0(c0,c1)  into aa(371);
forall i in t372.first..t372.last  insert into t0 values(t372(i).c0,t372(i).c1) return obj0(c0,c1)  into aa(372);
forall i in t373.first..t373.last  insert into t0 values(t373(i).c0,t373(i).c1) return obj0(c0,c1)  into aa(373);
forall i in t374.first..t374.last  insert into t0 values(t374(i).c0,t374(i).c1) return obj0(c0,c1)  into aa(374);
forall i in t375.first..t375.last  insert into t0 values(t375(i).c0,t375(i).c1) return obj0(c0,c1)  into aa(375);
forall i in t376.first..t376.last  insert into t0 values(t376(i).c0,t376(i).c1) return obj0(c0,c1)  into aa(376);
forall i in t377.first..t377.last  insert into t0 values(t377(i).c0,t376(i).c1) return obj0(c0,c1)  into aa(377);
forall i in t378.first..t378.last  insert into t0 values(t378(i).c0,t378(i).c1) return obj0(c0,c1)  into aa(378);
forall i in t379.first..t379.last  insert into t0 values(t379(i).c0,t379(i).c1) return obj0(c0,c1)  into aa(379);
forall i in t380.first..t380.last  insert into t0 values(t380(i).c0,t380(i).c1) return obj0(c0,c1)  into aa(380);
forall i in t381.first..t381.last  insert into t0 values(t381(i).c0,t381(i).c1) return obj0(c0,c1)  into aa(381);
forall i in t382.first..t382.last  insert into t0 values(t382(i).c0,t382(i).c1) return obj0(c0,c1)  into aa(382);
forall i in t383.first..t383.last  insert into t0 values(t383(i).c0,t383(i).c1) return obj0(c0,c1)  into aa(383);
forall i in t384.first..t384.last  insert into t0 values(t384(i).c0,t384(i).c1) return obj0(c0,c1)  into aa(384);
forall i in t385.first..t385.last  insert into t0 values(t385(i).c0,t385(i).c1) return obj0(c0,c1)  into aa(385);
forall i in t386.first..t386.last  insert into t0 values(t386(i).c0,t386(i).c1) return obj0(c0,c1)  into aa(386);
forall i in t387.first..t387.last  insert into t0 values(t387(i).c0,t386(i).c1) return obj0(c0,c1)  into aa(387);
forall i in t388.first..t388.last  insert into t0 values(t388(i).c0,t388(i).c1) return obj0(c0,c1)  into aa(388);
forall i in t389.first..t389.last  insert into t0 values(t389(i).c0,t389(i).c1) return obj0(c0,c1)  into aa(389);
forall i in t390.first..t390.last  insert into t0 values(t390(i).c0,t390(i).c1) return obj0(c0,c1)  into aa(390);
forall i in t391.first..t391.last  insert into t0 values(t391(i).c0,t391(i).c1) return obj0(c0,c1)  into aa(391);
forall i in t392.first..t392.last  insert into t0 values(t392(i).c0,t392(i).c1) return obj0(c0,c1)  into aa(392);
forall i in t393.first..t393.last  insert into t0 values(t393(i).c0,t393(i).c1) return obj0(c0,c1)  into aa(393);
forall i in t394.first..t394.last  insert into t0 values(t394(i).c0,t394(i).c1) return obj0(c0,c1)  into aa(394);
forall i in t395.first..t395.last  insert into t0 values(t395(i).c0,t395(i).c1) return obj0(c0,c1)  into aa(395);
forall i in t396.first..t396.last  insert into t0 values(t396(i).c0,t396(i).c1) return obj0(c0,c1)  into aa(396);
forall i in t397.first..t397.last  insert into t0 values(t397(i).c0,t396(i).c1) return obj0(c0,c1)  into aa(397);
forall i in t398.first..t398.last  insert into t0 values(t398(i).c0,t398(i).c1) return obj0(c0,c1)  into aa(398);
forall i in t399.first..t399.last  insert into t0 values(t399(i).c0,t399(i).c1) return obj0(c0,c1)  into aa(399);
forall i in t400.first..t400.last  insert into t0 values(t400(i).c0,t400(i).c1) return obj0(c0,c1)  into aa(400);
forall i in t401.first..t401.last  insert into t0 values(t401(i).c0,t41(i).c1) return obj0(c0,c1)  into aa(401);
forall i in t402.first..t402.last  insert into t0 values(t402(i).c0,t42(i).c1) return obj0(c0,c1)  into aa(402);
forall i in t403.first..t403.last  insert into t0 values(t403(i).c0,t43(i).c1) return obj0(c0,c1)  into aa(403);
forall i in t404.first..t404.last  insert into t0 values(t404(i).c0,t44(i).c1) return obj0(c0,c1)  into aa(404);
forall i in t405.first..t405.last  insert into t0 values(t405(i).c0,t45(i).c1) return obj0(c0,c1)  into aa(405);
forall i in t406.first..t406.last  insert into t0 values(t406(i).c0,t46(i).c1) return obj0(c0,c1)  into aa(406);
forall i in t407.first..t407.last  insert into t0 values(t407(i).c0,t46(i).c1) return obj0(c0,c1)  into aa(407);
forall i in t408.first..t408.last  insert into t0 values(t408(i).c0,t48(i).c1) return obj0(c0,c1)  into aa(408);
forall i in t409.first..t409.last  insert into t0 values(t409(i).c0,t49(i).c1) return obj0(c0,c1)  into aa(409);
forall i in t410.first..t410.last  insert into t0 values(t410(i).c0,t410(i).c1) return obj0(c0,c1)  into aa(410);
forall i in t411.first..t411.last  insert into t0 values(t411(i).c0,t411(i).c1) return obj0(c0,c1)  into aa(411);
forall i in t412.first..t412.last  insert into t0 values(t412(i).c0,t412(i).c1) return obj0(c0,c1)  into aa(412);
forall i in t413.first..t413.last  insert into t0 values(t413(i).c0,t413(i).c1) return obj0(c0,c1)  into aa(413);
forall i in t414.first..t414.last  insert into t0 values(t414(i).c0,t414(i).c1) return obj0(c0,c1)  into aa(414);
forall i in t415.first..t415.last  insert into t0 values(t415(i).c0,t415(i).c1) return obj0(c0,c1)  into aa(415);
forall i in t416.first..t416.last  insert into t0 values(t416(i).c0,t416(i).c1) return obj0(c0,c1)  into aa(416);
forall i in t417.first..t417.last  insert into t0 values(t417(i).c0,t416(i).c1) return obj0(c0,c1)  into aa(417);
forall i in t418.first..t418.last  insert into t0 values(t418(i).c0,t418(i).c1) return obj0(c0,c1)  into aa(418);
forall i in t419.first..t419.last  insert into t0 values(t419(i).c0,t419(i).c1) return obj0(c0,c1)  into aa(419);
forall i in t420.first..t420.last  insert into t0 values(t420(i).c0,t420(i).c1) return obj0(c0,c1)  into aa(420);
forall i in t421.first..t421.last  insert into t0 values(t421(i).c0,t421(i).c1) return obj0(c0,c1)  into aa(421);
forall i in t422.first..t422.last  insert into t0 values(t422(i).c0,t422(i).c1) return obj0(c0,c1)  into aa(422);
forall i in t423.first..t423.last  insert into t0 values(t423(i).c0,t423(i).c1) return obj0(c0,c1)  into aa(423);
forall i in t424.first..t424.last  insert into t0 values(t424(i).c0,t424(i).c1) return obj0(c0,c1)  into aa(424);
forall i in t425.first..t425.last  insert into t0 values(t425(i).c0,t425(i).c1) return obj0(c0,c1)  into aa(425);
forall i in t426.first..t426.last  insert into t0 values(t426(i).c0,t426(i).c1) return obj0(c0,c1)  into aa(426);
forall i in t427.first..t427.last  insert into t0 values(t427(i).c0,t426(i).c1) return obj0(c0,c1)  into aa(427);
forall i in t428.first..t428.last  insert into t0 values(t428(i).c0,t428(i).c1) return obj0(c0,c1)  into aa(428);
forall i in t429.first..t429.last  insert into t0 values(t429(i).c0,t429(i).c1) return obj0(c0,c1)  into aa(429);
forall i in t430.first..t430.last  insert into t0 values(t430(i).c0,t430(i).c1) return obj0(c0,c1)  into aa(430);
forall i in t431.first..t431.last  insert into t0 values(t431(i).c0,t431(i).c1) return obj0(c0,c1)  into aa(431);
forall i in t432.first..t432.last  insert into t0 values(t432(i).c0,t432(i).c1) return obj0(c0,c1)  into aa(432);
forall i in t433.first..t433.last  insert into t0 values(t433(i).c0,t433(i).c1) return obj0(c0,c1)  into aa(433);
forall i in t434.first..t434.last  insert into t0 values(t434(i).c0,t434(i).c1) return obj0(c0,c1)  into aa(434);
forall i in t435.first..t435.last  insert into t0 values(t435(i).c0,t435(i).c1) return obj0(c0,c1)  into aa(435);
forall i in t436.first..t436.last  insert into t0 values(t436(i).c0,t436(i).c1) return obj0(c0,c1)  into aa(436);
forall i in t437.first..t437.last  insert into t0 values(t437(i).c0,t436(i).c1) return obj0(c0,c1)  into aa(437);
forall i in t438.first..t438.last  insert into t0 values(t438(i).c0,t438(i).c1) return obj0(c0,c1)  into aa(438);
forall i in t439.first..t439.last  insert into t0 values(t439(i).c0,t439(i).c1) return obj0(c0,c1)  into aa(439);
forall i in t440.first..t440.last  insert into t0 values(t440(i).c0,t440(i).c1) return obj0(c0,c1)  into aa(440);
forall i in t441.first..t441.last  insert into t0 values(t441(i).c0,t441(i).c1) return obj0(c0,c1)  into aa(441);
forall i in t442.first..t442.last  insert into t0 values(t442(i).c0,t442(i).c1) return obj0(c0,c1)  into aa(442);
forall i in t443.first..t443.last  insert into t0 values(t443(i).c0,t443(i).c1) return obj0(c0,c1)  into aa(443);
forall i in t444.first..t444.last  insert into t0 values(t444(i).c0,t444(i).c1) return obj0(c0,c1)  into aa(444);
forall i in t445.first..t445.last  insert into t0 values(t445(i).c0,t445(i).c1) return obj0(c0,c1)  into aa(445);
forall i in t446.first..t446.last  insert into t0 values(t446(i).c0,t446(i).c1) return obj0(c0,c1)  into aa(446);
forall i in t447.first..t447.last  insert into t0 values(t447(i).c0,t446(i).c1) return obj0(c0,c1)  into aa(447);
forall i in t448.first..t448.last  insert into t0 values(t448(i).c0,t448(i).c1) return obj0(c0,c1)  into aa(448);
forall i in t449.first..t449.last  insert into t0 values(t449(i).c0,t449(i).c1) return obj0(c0,c1)  into aa(449);
forall i in t450.first..t450.last  insert into t0 values(t450(i).c0,t450(i).c1) return obj0(c0,c1)  into aa(450);
forall i in t451.first..t451.last  insert into t0 values(t451(i).c0,t451(i).c1) return obj0(c0,c1)  into aa(451);
forall i in t452.first..t452.last  insert into t0 values(t452(i).c0,t452(i).c1) return obj0(c0,c1)  into aa(452);
forall i in t453.first..t453.last  insert into t0 values(t453(i).c0,t453(i).c1) return obj0(c0,c1)  into aa(453);
forall i in t454.first..t454.last  insert into t0 values(t454(i).c0,t454(i).c1) return obj0(c0,c1)  into aa(454);
forall i in t455.first..t455.last  insert into t0 values(t455(i).c0,t455(i).c1) return obj0(c0,c1)  into aa(455);
forall i in t456.first..t456.last  insert into t0 values(t456(i).c0,t456(i).c1) return obj0(c0,c1)  into aa(456);
forall i in t457.first..t457.last  insert into t0 values(t457(i).c0,t456(i).c1) return obj0(c0,c1)  into aa(457);
forall i in t458.first..t458.last  insert into t0 values(t458(i).c0,t458(i).c1) return obj0(c0,c1)  into aa(458);
forall i in t459.first..t459.last  insert into t0 values(t459(i).c0,t459(i).c1) return obj0(c0,c1)  into aa(459);
forall i in t460.first..t460.last  insert into t0 values(t460(i).c0,t460(i).c1) return obj0(c0,c1)  into aa(460);
forall i in t461.first..t461.last  insert into t0 values(t461(i).c0,t461(i).c1) return obj0(c0,c1)  into aa(461);
forall i in t462.first..t462.last  insert into t0 values(t462(i).c0,t462(i).c1) return obj0(c0,c1)  into aa(462);
forall i in t463.first..t463.last  insert into t0 values(t463(i).c0,t463(i).c1) return obj0(c0,c1)  into aa(463);
forall i in t464.first..t464.last  insert into t0 values(t464(i).c0,t464(i).c1) return obj0(c0,c1)  into aa(464);
forall i in t465.first..t465.last  insert into t0 values(t465(i).c0,t465(i).c1) return obj0(c0,c1)  into aa(465);
forall i in t466.first..t466.last  insert into t0 values(t466(i).c0,t466(i).c1) return obj0(c0,c1)  into aa(466);
forall i in t467.first..t467.last  insert into t0 values(t467(i).c0,t466(i).c1) return obj0(c0,c1)  into aa(467);
forall i in t468.first..t468.last  insert into t0 values(t468(i).c0,t468(i).c1) return obj0(c0,c1)  into aa(468);
forall i in t469.first..t469.last  insert into t0 values(t469(i).c0,t469(i).c1) return obj0(c0,c1)  into aa(469);
forall i in t470.first..t470.last  insert into t0 values(t470(i).c0,t470(i).c1) return obj0(c0,c1)  into aa(470);
forall i in t471.first..t471.last  insert into t0 values(t471(i).c0,t471(i).c1) return obj0(c0,c1)  into aa(471);
forall i in t472.first..t472.last  insert into t0 values(t472(i).c0,t472(i).c1) return obj0(c0,c1)  into aa(472);
forall i in t473.first..t473.last  insert into t0 values(t473(i).c0,t473(i).c1) return obj0(c0,c1)  into aa(473);
forall i in t474.first..t474.last  insert into t0 values(t474(i).c0,t474(i).c1) return obj0(c0,c1)  into aa(474);
forall i in t475.first..t475.last  insert into t0 values(t475(i).c0,t475(i).c1) return obj0(c0,c1)  into aa(475);
forall i in t476.first..t476.last  insert into t0 values(t476(i).c0,t476(i).c1) return obj0(c0,c1)  into aa(476);
forall i in t477.first..t477.last  insert into t0 values(t477(i).c0,t476(i).c1) return obj0(c0,c1)  into aa(477);
forall i in t478.first..t478.last  insert into t0 values(t478(i).c0,t478(i).c1) return obj0(c0,c1)  into aa(478);
forall i in t479.first..t479.last  insert into t0 values(t479(i).c0,t479(i).c1) return obj0(c0,c1)  into aa(479);
forall i in t480.first..t480.last  insert into t0 values(t480(i).c0,t480(i).c1) return obj0(c0,c1)  into aa(480);
forall i in t481.first..t481.last  insert into t0 values(t481(i).c0,t481(i).c1) return obj0(c0,c1)  into aa(481);
forall i in t482.first..t482.last  insert into t0 values(t482(i).c0,t482(i).c1) return obj0(c0,c1)  into aa(482);
forall i in t483.first..t483.last  insert into t0 values(t483(i).c0,t483(i).c1) return obj0(c0,c1)  into aa(483);
forall i in t484.first..t484.last  insert into t0 values(t484(i).c0,t484(i).c1) return obj0(c0,c1)  into aa(484);
forall i in t485.first..t485.last  insert into t0 values(t485(i).c0,t485(i).c1) return obj0(c0,c1)  into aa(485);
forall i in t486.first..t486.last  insert into t0 values(t486(i).c0,t486(i).c1) return obj0(c0,c1)  into aa(486);
forall i in t487.first..t487.last  insert into t0 values(t487(i).c0,t486(i).c1) return obj0(c0,c1)  into aa(487);
forall i in t488.first..t488.last  insert into t0 values(t488(i).c0,t488(i).c1) return obj0(c0,c1)  into aa(488);
forall i in t489.first..t489.last  insert into t0 values(t489(i).c0,t489(i).c1) return obj0(c0,c1)  into aa(489);
forall i in t490.first..t490.last  insert into t0 values(t490(i).c0,t490(i).c1) return obj0(c0,c1)  into aa(490);
forall i in t491.first..t491.last  insert into t0 values(t491(i).c0,t491(i).c1) return obj0(c0,c1)  into aa(491);
forall i in t492.first..t492.last  insert into t0 values(t492(i).c0,t492(i).c1) return obj0(c0,c1)  into aa(492);
forall i in t493.first..t493.last  insert into t0 values(t493(i).c0,t493(i).c1) return obj0(c0,c1)  into aa(493);
forall i in t494.first..t494.last  insert into t0 values(t494(i).c0,t494(i).c1) return obj0(c0,c1)  into aa(494);
forall i in t495.first..t495.last  insert into t0 values(t495(i).c0,t495(i).c1) return obj0(c0,c1)  into aa(495);
forall i in t496.first..t496.last  insert into t0 values(t496(i).c0,t496(i).c1) return obj0(c0,c1)  into aa(496);
forall i in t497.first..t497.last  insert into t0 values(t497(i).c0,t496(i).c1) return obj0(c0,c1)  into aa(497);
forall i in t498.first..t498.last  insert into t0 values(t498(i).c0,t498(i).c1) return obj0(c0,c1)  into aa(498);
forall i in t499.first..t499.last  insert into t0 values(t499(i).c0,t499(i).c1) return obj0(c0,c1)  into aa(499);
forall i in t500.first..t500.last  insert into t0 values(t500(i).c0,t500(i).c1) return obj0(c0,c1)  into aa(500);
rollback;
end;
end;
/

CREATE or replace PROCEDURE EBANKYEARFEEUSERINFO_DEAL
AS
 --定义临时游标,存储临时欠费表数据
 TEMP_CURSOR SYS_REFCURSOR;
 --核心客户还
 V_EBUSER_NO VARCHAR2(20);
 --欠费账号
 V_ACCT_NO VARCHAR2(30);
 --欠费日期
 V_FEE_DATE VARCHAR2(20);
 --欠费流水
 V_FEE_SEQ  VARCHAR2(20);
 --欠费类型 1.证书年费 2-交易手续费
 V_FEE_TYPE VARCHAR2(5);
 --欠费金额
 V_FEE_AMT VARCHAR2(20);
 --数据日期
 V_TRAN_DATE VARCHAR2(20);
 --欠费标识 1.证书年费 2-交易手续费 3-集中收取
 V_FEE_FLAG VARCHAR2(5);
 --查询标志
 V_COUNT VARCHAR2(30);
BEGIN
 --回滚点
 SAVEPOINT PT1;
 BEGIN
 /*    --查询临时表数据
     OPEN TEMP_CURSOR
          FOR
             SELECT T.EBUSER_NO,T.ACCT_NO,T.FEE_DATE,T.FEE_SEQ,
               T.FEE_TYPE,T.FEE_AMT,T.TRAN_DATE,T.FEE_FLAG FROM QDZH.ARREARAGE_INFO_TMP T;
     LOOP
         FETCH TEMP_CURSOR INTO V_EBUSER_NO,V_ACCT_NO,V_FEE_DATE,V_FEE_SEQ,V_FEE_TYPE,V_FEE_AMT,
               V_TRAN_DATE,V_FEE_FLAG;--获得当前记录的数据
         EXIT WHEN TEMP_CURSOR%NOTFOUND;
         DBMS_OUTPUT.PUT_LINE('ARREARAGE_INFO_TMP FOUND DATA');
         --1.证书类型：
         --欠费日期 加欠费账号 去历史表查询，如果历史表有数据，则过滤
         --2 手续费 ：
         --欠费日期 加欠费流水 去历史表查询，如果历史表有数据，则过滤
         --3 集中收取 ：
         --欠费日期 加欠费账号 去历史表查询，如果历史表有数据，则过滤
         IF V_FEE_FLAG = '1' OR V_FEE_FLAG = '3' THEN
            BEGIN
             DBMS_OUTPUT.PUT_LINE('V_ACCT_NO : ' ||V_ACCT_NO || ',V_FEE_DATE : ' || V_FEE_DATE);
             --删除历史表有的数据
             DELETE FROM QDZH.ARREARAGE_INFO A WHERE A.ACCT_NO = V_ACCT_NO AND A.FEE_DATE = V_FEE_DATE AND A.FEE_TYPE = V_FEE_TYPE;

            END;
         ELSIF V_FEE_FLAG = '2' THEN
           BEGIN
             DBMS_OUTPUT.PUT_LINE('V_FEE_SEQ : ' ||V_FEE_SEQ || ',V_FEE_DATE : ' || V_FEE_DATE);
              --删除历史表有的数据
             DELETE FROM QDZH.ARREARAGE_INFO A WHERE A.FEE_SEQ = V_FEE_SEQ AND A.FEE_DATE = V_FEE_DATE AND A.FEE_TYPE = V_FEE_TYPE ;

           END;
         END IF;
         DBMS_OUTPUT.PUT_LINE('INSERT INTO  ARREARAGE_INFO');
         --新增当日数据
         INSERT INTO  (EBUSER_NO,ACCT_NO,FEE_DATE,FEE_SEQ,FEE_TYPE,FEE_AMT,TRAN_DATE)
                VALUES(V_EBUSER_NO,V_ACCT_NO,V_FEE_DATE,V_FEE_SEQ,V_FEE_TYPE,V_FEE_AMT,V_TRAN_DATE);
     END LOOP;  
*/
      DELETE FROM  ARREARAGE_INFO A WHERE  FEE_FLAG IN('1','3')
      AND  EXISTS(SELECT 1 FROM ARREARAGE_INFO_TMP B WHERE A.EBUSER_NO=B.EBUSER_NO AND A.FEE_DATE=B.FEE_DATE 
      AND B.FEE_FLAG IN('1','3'));
      
      DELETE FROM  ARREARAGE_INFO A WHERE FEE_FLAG = '2'
      AND  EXISTS(SELECT 1 FROM ARREARAGE_INFO_TMP B WHERE A.FEE_SEQ=B.FEE_SEQ AND A.FEE_DATE=B.FEE_DATE 
      AND FEE_FLAG = '2' );
     COMMIT;
     
     INSERT INTO ARREARAGE_INFO SELECT * FROM ARREARAGE_INFO_TMP;
     
     DELETE FROM  ARREARAGE_INFO A WHERE A.FEE_FLAG = '1' AND A.STATUS   NOT IN ('0','1','3');
     DELETE FROM  ARREARAGE_INFO A WHERE A.FEE_FLAG = '2' AND A.STATUS   NOT IN ('0');
     DELETE FROM  ARREARAGE_INFO A WHERE A.FEE_FLAG = '3' AND A.STATUS   NOT IN ('0','2');
     
     COMMIT;
 END;
 EXCEPTION WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
      COMMIT;
END;

/

create sequence FRONT_FILE_SEQ;

CREATE or replace PROCEDURE GET_TRANS_SEQLIST(seq_num in number,seq_list out varchar2)
as
seq varchar2(20);
z number;
begin
z:=0;
while z<seq_num
loop
        select FRONT_FILE_SEQ.Nextval into seq from dual;
        if (z=seq_num-1) then
         seq_list:=seq_list||seq;
        else
         seq_list:=seq_list||seq||',';
         end if;
        z:=z+1;
end loop;
end;

/

create index seleidex on dtb_roll_tr (oper_type,trans_chan_type,tran_state,host_code,ebuser_no);
/
create index upidx on dtb_acct (tranin_amt,acct_tp);/

create index upidx2 on dtb_acct (acct_no);
/

CREATE or replace PROCEDURE INIT_TRANINAMT is

begin
  declare l_pay_amt number;
  l_rec_amt number;
  l_card_no varchar2(100);
  l_ebuser_no varchar2(100);
  l_amt number;
  rs Sys_Refcursor;
  begin
    open rs for
     --select sum(oper_amt) ,card_no  from dtb_roll_tr t where oper_type='0' and trans_chan_type='1'
    -- and(tran_state='6' or host_code='000000') group by card_no;
    -- loop fetch rs into l_amt,l_card_no;
    select  /*+ PARALLEL(10),enable_parallel_dml*/ acct_no,ebuser_no from dtb_acct where ACCT_TP='1' and rownum < 100;
    loop fetch rs into l_card_no,l_ebuser_no;
     exit when rs%notfound;
     l_pay_amt:=0;
     l_rec_amt:=0;
      select sum(oper_amt) into l_pay_amt  from dtb_roll_tr t where oper_type='0' and trans_chan_type='1' and(tran_state='6' or host_code='000000')
      and ebuser_no=l_ebuser_no;
      select sum(oper_amt) into l_rec_amt  from dtb_roll_tr t where oper_type='1' and trans_chan_type='1' and(tran_state='7' or host_code='000000')
      and ebuser_no=l_ebuser_no;
      if(l_pay_amt<0 or l_pay_amt is null) then l_pay_amt:=0; end if;
      if(l_rec_amt<0 or l_rec_amt is null) then l_rec_amt:=0; end if;
      l_amt:=l_pay_amt-l_rec_amt;
      if(l_amt<0 or l_amt is null) then l_amt:='0'; 
  elsif (l_amt>99999999999999) then l_amt:=mod(l_amt,99999999999999); end if;
      dbms_output.put_line(l_card_no||':'||l_pay_amt||':'||l_rec_amt||':'||l_amt);

     execute immediate 'update dtb_acct set TRANIN_AMT='||l_amt||' where ACCT_NO=:accno ' using l_card_no;
     update dtb_acct set TRANIN_AMT='0' where tranin_amt is null and acct_tp='1';
     end loop;
     commit;
  end;
end INIT_TRANINAMT;
/

CREATE or replace PROCEDURE SOTP_SPEC_LIMIT_QUERY(V_CHAN_TYP            IN VARCHAR2, --渠道类型
                                                  V_EBUSER_NO           IN VARCHAR2, --电子银行客户号
                                                  V_TRANS_DATE          IN VARCHAR2, --转账日期
                                                  V_TRANS_TYPE          IN VARCHAR2, --特殊转账类型
                                               --   V_SPEC_LIMIT_LIST     OUT SYS_REFCURSOR, --特殊限额列表
                                                  V_IS_OPEN_SOTP        OUT VARCHAR2, --是否开通sotp 1-开通  0-不开通
                                                  V_SUM_DAY_LIMIT       OUT NUMBER, --特殊转账单日总额度
                                                  V_SUM_AVAIL_DAY_LIMIT OUT NUMBER) --特殊转账单日剩余额度
 AS

  V_EXCEPTION EXCEPTION; --自定义异常类型
  V_SPEC_LIMIT_LIST SYS_REFCURSOR;
  V_IS_HAS_SOTP VARCHAR2(1); -- 1:代表有sotp认证方式  0:代表没有sotp认证方式
  V_UNUSE       VARCHAR2(2); --无效变量
BEGIN
  V_IS_HAS_SOTP := '1'; --默认有sotp认证方式
  SAVEPOINT PT2; --回滚点
  BEGIN
    --查询sotp认证方式
    BEGIN
      SELECT SP.SEC_WAY
        INTO V_UNUSE
        FROM QDZH.P_SEC_AUTH SP
       WHERE SP.SEC_WAY = '00'
         AND SP.CHAN_TYP = V_CHAN_TYP
         AND SP.EBUSER_NO = V_EBUSER_NO
         AND SP.SEC_STATE = 'N';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_IS_HAS_SOTP := '0'; --无stop认证方式
    END;
    DBMS_OUTPUT.PUT_LINE('V_IS_HAS_SOTP ：' || V_IS_HAS_SOTP);
    --查询特殊转账单日总限额
    BEGIN
      --未开通sotp限额
      IF V_IS_HAS_SOTP = '0' THEN
        SELECT DAY_LIMIT
          INTO V_SUM_DAY_LIMIT
          FROM SPECI_TRANS_LIMIT
         WHERE TRANS_TYPE = 'ALL';
      ELSE
        SELECT DAY_LIMIT
          INTO V_SUM_DAY_LIMIT
          FROM SOTP_SPECI_TRANS_LIMIT
         WHERE TRANS_TYPE = 'ALL'
           AND STATUS = '0';
      END IF;
    END;
    --查询特殊转账单日可用限额
    BEGIN
      SELECT V_SUM_DAY_LIMIT - NVL(SUM(DAY_SUM_LIMIT), 0)
        INTO V_SUM_AVAIL_DAY_LIMIT
        FROM CUST_SPCI_LIM_USE
       WHERE EBUSER_NO = V_EBUSER_NO
         AND LAST_TRAN_DATE = V_TRANS_DATE;
    END;
    --查询特殊转账类型交易限额
    BEGIN
      --未开通sotp限额
      IF V_IS_HAS_SOTP = '0' THEN
        --转账类型为空，则查询全部非all的类型数据
        IF V_TRANS_TYPE IS NULL THEN
          OPEN V_SPEC_LIMIT_LIST FOR
            SELECT  /*+ PARALLEL(10),enable_parallel_dml*/ A.TRANS_TYPE,
                   A.ONCE_LIMIT,
                   NVL(C.DAY_SUM_LIMIT, 0) AS AVAIL_DAY_LIMIT
              FROM QDZH.SPECI_TRANS_LIMIT A
              LEFT JOIN (SELECT B.TRANS_TYPE, B.DAY_SUM_LIMIT
                           FROM QDZH.CUST_SPCI_LIM_USE B
                          WHERE B.EBUSER_NO = V_EBUSER_NO
                            AND B.LAST_TRAN_DATE = V_TRANS_DATE) C ON A.TRANS_TYPE =
                                                                      C.TRANS_TYPE
             WHERE A.TRANS_TYPE != 'ALL';
        ELSE
          OPEN V_SPEC_LIMIT_LIST FOR
            SELECT  /*+ PARALLEL(10),enable_parallel_dml*/ A.TRANS_TYPE,
                   A.ONCE_LIMIT,
                   NVL(C.DAY_SUM_LIMIT, 0) AS AVAIL_DAY_LIMIT
              FROM QDZH.SPECI_TRANS_LIMIT A
              LEFT JOIN (SELECT B.TRANS_TYPE, B.DAY_SUM_LIMIT
                           FROM QDZH.CUST_SPCI_LIM_USE B
                          WHERE B.EBUSER_NO = V_EBUSER_NO
                            AND B.LAST_TRAN_DATE = V_TRANS_DATE) C ON A.TRANS_TYPE =
                                                                      C.TRANS_TYPE
             WHERE A.TRANS_TYPE != 'ALL'
               AND A.TRANS_TYPE = V_TRANS_TYPE;
        END IF;

      ELSE

        --转账类型为空，则查询全部非all的类型数据
        IF V_TRANS_TYPE IS NULL THEN
          OPEN V_SPEC_LIMIT_LIST FOR
            SELECT  /*+ PARALLEL(10),enable_parallel_dml*/ A.TRANS_TYPE,
                   A.ONCE_LIMIT,
                   NVL(C.DAY_SUM_LIMIT, 0) AS AVAIL_DAY_LIMIT
              FROM QDZH.SOTP_SPECI_TRANS_LIMIT A
              LEFT JOIN (SELECT B.TRANS_TYPE, B.DAY_SUM_LIMIT
                           FROM QDZH.CUST_SPCI_LIM_USE B
                          WHERE B.EBUSER_NO = V_EBUSER_NO
                            AND B.LAST_TRAN_DATE = V_TRANS_DATE) C ON A.TRANS_TYPE =
                                                                      C.TRANS_TYPE
             WHERE A.TRANS_TYPE != 'ALL'
               AND A.STATUS = '0';
        ELSE
          OPEN V_SPEC_LIMIT_LIST FOR
            SELECT  /*+ PARALLEL(10),enable_parallel_dml*/ A.TRANS_TYPE,
                   A.ONCE_LIMIT,
                   NVL(C.DAY_SUM_LIMIT, 0) AS AVAIL_DAY_LIMIT
              FROM QDZH.SOTP_SPECI_TRANS_LIMIT A
              LEFT JOIN (SELECT B.TRANS_TYPE, B.DAY_SUM_LIMIT
                           FROM QDZH.CUST_SPCI_LIM_USE B
                          WHERE B.EBUSER_NO = V_EBUSER_NO
                            AND B.LAST_TRAN_DATE = V_TRANS_DATE) C ON A.TRANS_TYPE =
                                                                      C.TRANS_TYPE
             WHERE A.TRANS_TYPE != 'ALL'
               AND A.STATUS = '0'
               AND A.TRANS_TYPE = V_TRANS_TYPE;
        END IF;
      END IF;
    END;
    V_IS_OPEN_SOTP := V_IS_HAS_SOTP;
    COMMIT;
  END;
EXCEPTION
  WHEN V_EXCEPTION THEN
    DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
    DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
    ROLLBACK TO SAVEPOINT PT2;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('EXCEPTION END');

END SOTP_SPEC_LIMIT_QUERY;
/

CREATE OR REPLACE PROCEDURE TEST1 is
  cursor c1 is
    select * from ctp_info;
begin
  for r1 in c1 loop
    begin
      update ctp_info set ctp_id=r1.ctp_id where ctp_id=r1.ctp_id;
    end;
  end loop;
end test1;
/

CREATE or replace PROCEDURE TRANS_LMT_CTP(
                          V_CHAN_TYP   IN VARCHAR2,
                          V_CTP_ID     IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          V_ONCE_LIMIT OUT VARCHAR2,--单笔限额
                          V_DAY_LIMIT OUT VARCHAR2,--单日限额
                          V_TIMES_LIMIT OUT VARCHAR2,--单日可以转出次数
                          LEFT_LIMIT OUT VARCHAR2,--当前可用限额
                          LEFT_TIMES OUT VARCHAR2,--当前可用转出次数
                          OUT_STATUS OUT VARCHAR2) --返回状态
AS
 CHAN_TYP   VARCHAR2(10);
 EBUSER_NO  VARCHAR2(50);
 TRAN_AMT   NUMBER(18,4);
 TRAN_TIMES NUMBER;-- 已转出次数
 TRAN_DATE  VARCHAR2(20);
 ONCE_LIMIT NUMBER(18,4);
 DAY_LIMIT  NUMBER(18,4);
 DAY_TIMES  NUMBER;--转出次数限制
 V_EXCEPTION EXCEPTION;
 --OUT_STATUS  0通过  1超过单笔限额 2超过单日限额 3无默认限额 4超过单日可转出次数
 BEGIN
   OUT_STATUS:=0;--0通过
   SAVEPOINT PT;
  BEGIN
  SELECT TRAN_AMT,CHAN_TYP,TRAN_TIMES INTO TRAN_AMT,CHAN_TYP,TRAN_TIMES FROM DTB_TRANS_LIMIT_USE
  WHERE EBUSER_NO=V_EBUSER_NO AND CHAN_TYP=V_CHAN_TYP  AND TRAN_DATE=V_TRANS_DATE;
  EXCEPTION
         WHEN NO_DATA_FOUND THEN
           TRAN_AMT := '0'; --无数据时，历史交易金额默认为0
           TRAN_TIMES := '0';--无数据时，当天转出次数为0
         DBMS_OUTPUT.PUT_LINE('查询交易金额：'||TRAN_AMT||' 转出次数：'||TRAN_TIMES);
  END;
   BEGIN
    SELECT ONCE_LIMIT,DAY_LIMIT,DAY_TIMES INTO ONCE_LIMIT,DAY_LIMIT,DAY_TIMES FROM DTB_TRANS_LIMIT
    WHERE CHAN_TYP=V_CHAN_TYP AND CTP_ID=V_CTP_ID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
         BEGIN
           SELECT ONCE_LIMIT,DAY_LIMIT,DAY_TIMES INTO ONCE_LIMIT,DAY_LIMIT,DAY_TIMES FROM DTB_TRANS_LIMIT
           WHERE CHAN_TYP=V_CHAN_TYP AND CTP_ID='ALL';
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
             OUT_STATUS := '3'; --3:无默认限额
             DBMS_OUTPUT.PUT_LINE('默认限额未设置');
          END;
       END;
    V_ONCE_LIMIT:=CAST(ONCE_LIMIT AS VARCHAR2);
    V_DAY_LIMIT:=CAST(DAY_LIMIT AS VARCHAR2);
    V_TIMES_LIMIT:=CAST(DAY_TIMES AS VARCHAR2);
    LEFT_LIMIT:=CAST(DAY_LIMIT-TRAN_AMT AS VARCHAR2);
    LEFT_TIMES:=CAST(DAY_TIMES-TRAN_TIMES AS VARCHAR2);
    DBMS_OUTPUT.PUT_LINE('单笔限额'||ONCE_LIMIT);
    DBMS_OUTPUT.PUT_LINE('单日限额'||DAY_LIMIT);
    DBMS_OUTPUT.PUT_LINE('单日转出次数'||DAY_TIMES);
    DBMS_OUTPUT.PUT_LINE('单日可用限额'||LEFT_LIMIT);
    DBMS_OUTPUT.PUT_LINE('单日剩余转出次数'||LEFT_TIMES);
    DBMS_OUTPUT.PUT_LINE('最终交易状态'||OUT_STATUS);
    DBMS_OUTPUT.PUT_LINE('最终累计限额'||TRAN_AMT);
    DBMS_OUTPUT.PUT_LINE('最终转出次数'||TRAN_TIMES);
    IF ONCE_LIMIT<V_TRANS_AMT THEN
       OUT_STATUS :='1';--1超过单笔限额
       RAISE V_EXCEPTION;
     RETURN;
    ELSIF DAY_LIMIT<TRAN_AMT+V_TRANS_AMT then
        OUT_STATUS :='2';--2超过单日限额
        RAISE V_EXCEPTION;
    ELSIF DAY_TIMES<TRAN_TIMES+1 then
        OUT_STATUS :='4';--4超过单日可转出次数
        RAISE V_EXCEPTION;
    RETURN;
    END IF;
    --更新用户限额
    TRAN_AMT:=TRAN_AMT+V_TRANS_AMT;
    TRAN_TIMES:=TRAN_TIMES+1;
    DELETE FROM DTB_TRANS_LIMIT_USE
    WHERE EBUSER_NO=V_EBUSER_NO
      AND CHAN_TYP=V_CHAN_TYP;
    INSERT INTO DTB_TRANS_LIMIT_USE(EBUSER_NO,CHAN_TYP,TRAN_DATE,TRAN_AMT,TRAN_TIMES)
      VALUES(V_EBUSER_NO,V_CHAN_TYP,V_TRANS_DATE,TRAN_AMT,TRAN_TIMES);
    IF OUT_STATUS<>'0' THEN
     RAISE V_EXCEPTION;
    ELSE
      COMMIT;
   END IF;
   EXCEPTION
      WHEN V_EXCEPTION  THEN
        DBMS_OUTPUT.PUT_LINE('EXCEPTION START');
        DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
        DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
        ROLLBACK TO SAVEPOINT PT;
        COMMIT;
      WHEN OTHERS THEN
          ROLLBACK TO SAVEPOINT PT;
          OUT_STATUS := sqlerrm;
          DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
          DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
          COMMIT;
    END;
/

CREATE or replace PROCEDURE TRANS_LMT_DTB(
                          V_CHAN_TYP   IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          V_ONCE_LIMIT OUT VARCHAR2,--单笔限额
                          V_DAY_LIMIT OUT VARCHAR2,--单日限额
                          LEFT_LIMIT OUT VARCHAR2,--当前可用限额
                          OUT_STATUS OUT VARCHAR2) --返回状态
AS
 CHAN_TYP   VARCHAR2(10);
 EBUSER_NO  VARCHAR2(50);
 TRAN_AMT   NUMBER(18,4);
 TRAN_DATE  VARCHAR2(20);
 ONCE_LIMIT NUMBER(18,4);
 DAY_LIMIT  NUMBER(18,4);
 V_EXCEPTION EXCEPTION;
 --OUT_STATUS  0通过  1超过单笔限额 2超过单日限额 3无默认限额
 BEGIN
   OUT_STATUS:=0;--0通过
   SAVEPOINT PT;
  BEGIN
  SELECT TRAN_AMT,CHAN_TYP INTO TRAN_AMT,CHAN_TYP FROM DTB_TRANS_LIMIT_USE
  WHERE EBUSER_NO=V_EBUSER_NO AND CHAN_TYP=V_CHAN_TYP  AND TRAN_DATE=V_TRANS_DATE;
  EXCEPTION
         WHEN NO_DATA_FOUND THEN
         TRAN_AMT := '0';--无数据时，历史交易金额默认为0
         DBMS_OUTPUT.PUT_LINE('查询交易金额'||TRAN_AMT);
  END;
   BEGIN
    SELECT ONCE_LIMIT,DAY_LIMIT INTO ONCE_LIMIT,DAY_LIMIT FROM DTB_TRANS_LIMIT
    WHERE CHAN_TYP=V_CHAN_TYP and CTP_ID='62826654';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
         OUT_STATUS := '3'; --3:无默认限额
         DBMS_OUTPUT.PUT_LINE('默认限额未设置');
      END;
     V_ONCE_LIMIT:=CAST(ONCE_LIMIT AS VARCHAR2);
     V_DAY_LIMIT:=CAST(DAY_LIMIT AS VARCHAR2);
     LEFT_LIMIT:=CAST(DAY_LIMIT-TRAN_AMT AS VARCHAR2);
      DBMS_OUTPUT.PUT_LINE('单笔限额'||ONCE_LIMIT);
      DBMS_OUTPUT.PUT_LINE('单日限额'||DAY_LIMIT);
      DBMS_OUTPUT.PUT_LINE('单前可用限额'||LEFT_LIMIT);
    IF ONCE_LIMIT<V_TRANS_AMT THEN
       OUT_STATUS :='1';--1超过单笔限额
       RAISE V_EXCEPTION;
     RETURN;
    ELSIF DAY_LIMIT<TRAN_AMT+V_TRANS_AMT then
        OUT_STATUS :='2';--2超过单日限额
        RAISE V_EXCEPTION;
    RETURN;
    END IF;
    --更新用户限额
    TRAN_AMT:=TRAN_AMT+V_TRANS_AMT;
    DELETE FROM DTB_TRANS_LIMIT_USE WHERE EBUSER_NO=V_EBUSER_NO
    AND CHAN_TYP=V_CHAN_TYP;
    INSERT INTO DTB_TRANS_LIMIT_USE(EBUSER_NO,CHAN_TYP,TRAN_DATE,TRAN_AMT)
    VALUES(V_EBUSER_NO,V_CHAN_TYP,V_TRANS_DATE,TRAN_AMT);
    IF OUT_STATUS<>'0' THEN
     RAISE V_EXCEPTION;
    ELSE
      COMMIT;
   END IF;
    DBMS_OUTPUT.PUT_LINE('最终交易状态'||OUT_STATUS);
    DBMS_OUTPUT.PUT_LINE('最终累计限额'||TRAN_AMT);
   EXCEPTION
      WHEN V_EXCEPTION  THEN
        DBMS_OUTPUT.PUT_LINE('EXCEPTION START');
        DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
        DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
        ROLLBACK TO SAVEPOINT PT;
        COMMIT;
      WHEN OTHERS THEN
          ROLLBACK TO SAVEPOINT PT;
          OUT_STATUS := sqlerrm;
          DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
          DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
          COMMIT;
    END;
/

