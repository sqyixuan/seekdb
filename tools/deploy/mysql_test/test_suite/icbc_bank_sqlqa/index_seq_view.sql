create sequence MIC.AB_EXERCISE_SEQ
minvalue 1
maxvalue 999999999999
start with 1741
increment by 1
cache 500
cycle;

create sequence MIC.AB_NOTICE_SEQ
minvalue 1
maxvalue 999999999999
start with 2241
increment by 1
cache 500
cycle;

create sequence MIC.AB_REPORT_SEQ
minvalue 1
maxvalue 999999999999
start with 1561
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_AUTHAPPLY_COMMENT_SEQ
minvalue 1
maxvalue 99999999
start with 7003
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_AUTHAPPLY_LOG_SEQ
minvalue 1
maxvalue 99999999
start with 361
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_AUTHORITYAPPLY_INFO_SEQ
minvalue 1
maxvalue 99999999
start with 6502
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_AUTHORITYAPPLY_SET_SEQ
minvalue 1
maxvalue 99999999
start with 48681
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_FILE_ID_SEQ
minvalue 1
maxvalue 99999999999999999999
start with 2081
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_FILE_REF_SEQ
minvalue 1
maxvalue 999999999999
start with 16541
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_HOLIDAYSET_SEQ
minvalue 1
maxvalue 999999999
start with 24206
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_MAIL_SEQ
minvalue 1
maxvalue 999999999999
start with 11892052
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_QXSQ_SEQ
minvalue 1
maxvalue 99999999
start with 6622
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_ROLE_SEQ
minvalue 1
maxvalue 99999999
start with 21
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_SAFETY_TEMPLATE_SEQ
minvalue 1
maxvalue 99999999
start with 1801
increment by 1
cache 500
cycle;
create sequence MIC.DAMS_SYSLOG_SEQ
minvalue 1
maxvalue 99999999999999999999
start with 31247
increment by 1
cache 500
cycle;

create sequence MIC.DAMS_TEMPLATE_SYS_REL_SEQ
minvalue 1
maxvalue 99999999
start with 921
increment by 1
cache 500
cycle;

create sequence MIC.DOC_SEQ
minvalue 1
maxvalue 99999999
start with 34652
increment by 1
cache 500
cycle;
create sequence MIC.GROUP_SEQ
minvalue 1
maxvalue 99999999
start with 461
increment by 1
cache 500
cycle;

create sequence MIC.LOG_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 122079604
increment by 1
cache 500
cycle;

create sequence MIC.SEQ_COM_BLOB_UID
minvalue 10000
maxvalue 99999999999999999999
start with 10180
increment by 1
cache 500
cycle;

create sequence MIC.SEQ_READ_EMPOWER
minvalue 1
maxvalue 99999999999999999999
start with 38501
increment by 1
cache 500
cycle;

create sequence MIC.SEQ_SU_VACATION_UPD
minvalue 1
maxvalue 9999999999
start with 15282
increment by 1
cache 500
cycle;

create sequence MIC.SE_SEQ
minvalue 1
maxvalue 9999999
start with 1401
increment by 1
cache 500
cycle;

create sequence MIC.SP_SEQ
minvalue 1
maxvalue 99999999
start with 2108
increment by 1
cache 500
cycle;

create sequence MIC.TRANLOGSEQUENCE
minvalue 1
maxvalue 100000000000000000000
start with 100000
increment by 1
cache 500
cycle;

CREATE OR REPLACE  VIEW MIC.VIW_DAMS_WF_ALL_PROCESS AS
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
        '0' CLOSE_FLAG
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
       '1' CLOSE_FLAG
FROM   dams_wf_hist_process t2
WHERE t2.valid_flag = 1
;

CREATE OR REPLACE  VIEW MIC.VIW_DAMS_WF_ALL_TASK AS
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
       '0' CLOSE_FLAG,
       t1.task_name task_name1
  FROM dams_wf_task t1
UNION ALL
SELECT t2.TASK_ID,
       t2.BUSINESS_ID,
       '???????' TASK_NAME,
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
       '1' CLOSE_FLAG,
       t2.task_name task_name1
  FROM dams_wf_hist_task t2
;