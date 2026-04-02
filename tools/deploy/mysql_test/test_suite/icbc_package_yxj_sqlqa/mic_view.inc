CREATE OR REPLACE VIEW "MIC"."VIW_DAMS_WF_ALL_TASK" (
	"TASK_ID",
	"BUSINESS_ID",
	"TASK_NAME",
	"SSIC_ID",
	"ROLE_ID",
	"STRU_ID",
	"PREV_TASK_ID",
	"PREV_SSIC_ID",
	"PREV_ROLE_ID",
	"PREV_STRU_ID",
	"FORM_ID",
	"CREATE_TIME",
	"TAKE_TIME",
	"END_TIME",
	"TASK_STATE",
	"OUTGOING_NAME",
	"ACTION_FLAG",
	"OPINION",
	"IS_FIRST",
	"PROCINST_ID",
	"AUTH_STATE",
	"SSIC_ID_TRAN",
	"SYS_CODE",
	"ACTIVITY_ID",
	"CLOSE_FLAG",
	"TASK_NAME1") AS 
SELECT t1.TASK_ID,
       t1.BUSINESS_ID,
       t1.TASK_NAME,
       t1.SSIC_ID,
       t1.ROLE_ID,
       t1.STRU_ID,
       t1.PREV_TASK_ID,
       t1.PREV_SSIC_ID,
       t1.PREV_ROLE_ID,
       t1.PREV_STRU_ID,
       t1.FORM_ID,
       t1.CREATE_TIME,
       t1.TAKE_TIME,
       t1.END_TIME,
       t1.TASK_STATE,
       t1.OUTGOING_NAME,
       t1.ACTION_FLAG,
       t1.OPINION,
       t1.IS_FIRST,
       t1.PROCINST_ID,
             T1.AUTH_STATE,
             T1.SSIC_ID_TRAN,
             t1.sys_code,
       t1.activity_id,
       '0' CLOSE_FLAG, --���
       t1.task_name task_name1
  FROM dams_wf_task t1
UNION ALL
SELECT t2.TASK_ID,
       t2.BUSINESS_ID,
       '���̽���' TASK_NAME,
       t2.SSIC_ID,
       t2.ROLE_ID,
       t2.STRU_ID,
       t2.PREV_TASK_ID,
       t2.PREV_SSIC_ID,
       t2.PREV_ROLE_ID,
       t2.PREV_STRU_ID,
       t2.FORM_ID,
       t2.CREATE_TIME,
       t2.TAKE_TIME,
       t2.END_TIME,
       t2.TASK_STATE,
       t2.OUTGOING_NAME,
       t2.ACTION_FLAG,
       t2.OPINION,
       t2.IS_FIRST,
       t2.PROCINST_ID,
             T2.AUTH_STATE,
             T2.SSIC_ID_TRAN,
             t2.sys_code,
       t2.activity_id,
       '1' CLOSE_FLAG, --�Ѱ�
       t2.task_name task_name1
  FROM dams_wf_hist_task t2
;


CREATE OR REPLACE VIEW "MIC"."VIW_SP_HCDEV_REL" (
	"HC_TYPE",
	"DEV_BRAND",
	"HC_DEV",
	"DEV_SPEC",
	"HC_NAME",
	"HC_SPEC") AS 
SELECT SSD.DICTCODE HC_TYPE,
       A.DEV_BRAND  DEV_BRAND,
       SD.DICTCODE  HC_DEV,
       D.DICTCODE   DEV_SPEC,
       T.HC_NAME    HC_NAME,
       T.HC_SPEC    HC_SPEC
  FROM DAMS_GEN_DICT D, --设备型号
       (SELECT REL.DEV_SPEC, LD.DICTCODE HC_NAME, LLD.DICTCODE HC_SPEC
          FROM SA_PRM_DEVHC_REL REL, --
               DAMS_GEN_DICT    LD, --耗材名称
               DAMS_GEN_DICT    LLD --耗材规格
         WHERE REL.HC_NAME = LD.DICTCODE
           AND LD.DICTTYPE = 'HC_NAME'
           AND LD.SYS_CODE = 'SP'
           AND LD.DICTLANG = 'zh_CN'
           AND REL.HC_SPEC = LLD.DICTCODE
           AND LLD.DICTTYPE = 'HC_SPEC'
           AND LLD.SYS_CODE = 'SP'
           AND LLD.DICTLANG = 'zh_CN'
           AND LLD.PARENT_CODE = LD.DICTCODE --此条件检查耗材名称和耗材规格符合父子关系
        ) T,
       SA_PRM_DEVBRD_REL A, --品牌
       DAMS_GEN_DICT SD, --设备名称
       DAMS_GEN_DICT SSD --耗材类型
 WHERE D.DICTCODE = T.DEV_SPEC(+)
   AND D.DICTTYPE = 'DEV_SPEC'
   AND D.VALID_FLAG = '1'
   AND D.SYS_CODE = 'SP'
   AND D.DICTLANG = 'zh_CN'
   AND D.DICTCODE = A.DEV_SPEC(+)
   AND D.PARENT_CODE = SD.DICTCODE
   AND SD.DICTTYPE = 'HC_DEV'
   AND SD.SYS_CODE = 'SP'
   AND SD.DICTLANG = 'zh_CN'
   AND SD.VALID_FLAG = '1'
   AND SD.PARENT_CODE = SSD.DICTCODE
   AND SSD.DICTTYPE = 'HC_TYPE'
   AND SSD.SYS_CODE = 'SA'
   AND SSD.DICTLANG = 'zh_CN'
   AND SSD.VALID_FLAG = '1';




CREATE OR REPLACE VIEW "MIC"."VIW_DAMS_WF_ALL_PROCESS" (
	"BUSINESS_ID",
	"CFG_ID",
	"BUSINESS_TYPE",
	"PROCESS_ID",
	"PROCINST_ID",
	"TITLE",
	"PROCESS_NAME",
	"CREATOR_ID",
	"START_TIME",
	"COMPLETE_TIME",
	"CREATOR_STRU",
	"VALID_FLAG",
	"SYS_CODE",
	"CLOSE_FLAG") AS 
SELECT business_id,
       cfg_id,
       business_type,
       process_id,
       procinst_id,
       title,
       process_name,
       creator_id,
       start_time,
       complete_time,
       creator_stru,
       valid_flag,
       sys_code,
        '0' CLOSE_FLAG --���
FROM   dams_wf_process t1
WHERE t1.valid_flag = 1
UNION ALL
SELECT business_id,
       cfg_id,
       business_type,
       process_id,
       procinst_id,
       title,
       process_name,
       creator_id,
       start_time,
       complete_time,
       creator_stru,
       valid_flag,
       sys_code,
       '1' CLOSE_FLAG --���
FROM   dams_wf_hist_process t2
WHERE t2.valid_flag = 1;




CREATE OR REPLACE VIEW "MIC"."USER_ROLE" (
	"NAME",
	"SSIC_ID",
	"ROLE_ID",
	"ROLE_NAME",
	"STRU_ID",
	"STRU_NAME") AS 
select u.name, ur.ssic_id, ur.role_id, r.role_name, u.stru_id, u.stru_name from dams_user_role_rel ur, dams_role_nls r, dams_user u where ur.role_id = r.role_id and r.role_id = 'SP006'
and u.ssic_id = ur.ssic_id;




CREATE OR REPLACE VIEW "MIC"."VIW_CITY_LIST" (
	"DICTTYPE",
	"CITY_ID",
	"CITY_NAME",
	"CITY_PY",
	"CITY_SORT") AS 
SELECT 'CITY' dicttype,city_id,city_name,city_py,city_sort
   FROM dams_payment_position;



CREATE OR REPLACE VIEW "MIC"."VIW_DAMS_INNER_ALL_TASK" (
	"TASK_ID",
	"SSIC_ID",
	"PREV_TASK_NAME",
	"PREV_SSIC_ID",
	"CREATE_TIME",
	"TAKE_TIME",
	"END_TIME",
	"TASK_STATE",
	"BUSINESS_ID",
	"BUSINESS_TYPE",
	"TITLE",
	"CREATOR_ID",
	"SYS_CODE",
	"URL",
	"IN_SYS_SEQ",
	"CREATOR_STRU",
	"CLOSE_FLAG") AS 
SELECT t1.task_id,
       t1.ssic_id,
       t1.prev_task_name,
       t1.prev_ssic_id,
       t1.create_time,
       t1.take_time,
       t1.end_time,
       t1.task_state,
       t1.business_id,
       t1.business_type,
       t1.title,
       t1.creator_id,
       t1.sys_code,
       t1.url,
       t1.in_sys_seq,
       t1.creator_stru,
       '0' close_flag --待办
  FROM dams_inner_todo_task t1
UNION ALL
SELECT t2.task_id,
       t2.ssic_id,
       t2.prev_task_name,
       t2.prev_ssic_id,
       t2.create_time,
       t2.take_time,
       t2.end_time,
       t2.task_state,
       t2.business_id,
       t2.business_type,
       t2.title,
       t2.creator_id,
       t2.sys_code,
       t2.url,
       t2.in_sys_seq,
       t2.creator_stru,
       '1' close_flag --已办
  FROM dams_inner_over_task t2;


CREATE OR REPLACE VIEW "MIC"."VIW_SO_DICTRANGE" (
	"DICTTYPE",
	"DICTCODE",
	"DICTVALUE",
	"DICTCODE1",
	"DICTTYPE1") AS 
(SELECT t.dicttype,t.dictcode,t.dictvalue,t.dicttype||'$|$'||t.dictcode dictcode1,'SO_PROM' dicttype1 FROM dams_gen_dict t,sa_prm_dev_define t1 where t.dicttype = 'DEV_NAME' AND t.sys_code = 'SO' AND t.dictcode = t1.dev_type AND t1.secure_flag = '1' AND t1.valid_flag = '1' AND t.valid_flag = '1' AND t.dictlang = 'zh_CN')
UNION
--特殊耗材字典
(SELECT t.dicttype,t.dictcode,t.dictvalue,t.dicttype||'$|$'||t.dictcode dictcode1,'SO_PROM' dicttype1 FROM dams_gen_dict t where t.dicttype = 'HC_NAME' AND t.sys_code = 'SP' AND t.parent_code = '02' AND t.sys_code = 'SP' AND t.valid_flag = '1' AND t.dictlang = 'zh_CN')
UNION
--数字证书字典
(SELECT t.dicttype,t.dictcode,t.dictvalue,t.dicttype||'$|$'||t.dictcode dictcode1,'SO_PROM' dicttype1 FROM dams_gen_dict t where t.dicttype = 'HC_TYPE' AND t.sys_code = 'SA' AND t.dictcode IN ('03','04') AND t.valid_flag = '1' AND t.dictlang = 'zh_CN' AND t.sys_code = 'SA')
;



CREATE OR REPLACE VIEW "MIC"."VIW_SX_VEHICLE" (
	"DICTTYPE",
	"DICTCODE",
	"VEHICLE_ID",
	"VEHICLE",
	"LEVEL_ID",
	"VEHICLE_LEVEL",
	"DICTLANG",
	"SORT") AS 
SELECT 'VEHICLE' DICTTYPE, ROWNUM DICTCODE ,t.vehicle_id, tv.dictvalue vehicle, t.level_id, tl.dictvalue vehicle_level, tv.dictlang dictlang, t.sort
  FROM sx_vehicle_standard t, dams_gen_dict tv, dams_gen_dict tl
 WHERE tv.dicttype = 'VEHICLE'
   AND tv.sys_code = 'SX'
   AND t.vehicle_id = tv.dictcode
   AND tl.dicttype = 'VEHICLE_LEVEL'
   AND tl.dictlang = 'zh_CN'
   AND t.level_id = tl.dictcode
   AND tv.dictlang = tl.dictlang
   ORDER BY tl.order_no
;


create sequence aaa_coverage_seq;

