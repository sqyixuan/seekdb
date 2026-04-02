DELIMITER //;
CREATE OR REPLACE PACKAGE PACK_LOG AS
  START_STEP CONSTANT DB_LOG.STEP_NO%TYPE := '0';  --开始步点
  END_STEP   CONSTANT DB_LOG.STEP_NO%TYPE := '-1'; --结束步点

  START_MSG  CONSTANT VARCHAR2(10) := '开始！'; --开始信息
  END_MSG    CONSTANT VARCHAR2(10) := '结束！'; --结束信息

  DEBUG_LEVEL CONSTANT DB_LOG.LOG_LEVEL%TYPE := '2'; --调试情况
  INFO_LEVEL  CONSTANT DB_LOG.LOG_LEVEL%TYPE := '3'; --正常情况
  WARN_LEVEL  CONSTANT DB_LOG.LOG_LEVEL%TYPE := '4'; --数据错误，可预知错误
  ERR_LEVEL   CONSTANT DB_LOG.LOG_LEVEL%TYPE := '5'; --异常错误，未知错误

  PROCEDURE LOG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                IN_STEPNO   IN DB_LOG.STEP_NO%TYPE,   -- 步骤名
                IN_INFO     IN DB_LOG.INFO%TYPE,      -- 日志级别
                IN_LEVEL    IN DB_LOG.LOG_LEVEL%TYPE);-- 级别

  PROCEDURE DEBUG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                  IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                  IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE INFO(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE WARN(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE ERROR(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

END;
//

CREATE OR REPLACE PACKAGE BODY PACK_LOG AS
  LOG_LEVEL_FILTER  DB_LOG.LOG_LEVEL%TYPE := '3';
  LOGGING_EXCEPTION EXCEPTION;
  
  PROCEDURE LOG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                IN_INFO     IN DB_LOG.INFO%TYPE, --日志级别
                IN_LEVEL    IN DB_LOG.LOG_LEVEL%TYPE -- 级别
                ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    V_ERRSTACK VARCHAR2(4000);
    V_ERROR_BACKTRACE VARCHAR2(4000);
    V_INFO VARCHAR2(4000);
  BEGIN
    IF (IN_LEVEL >= LOG_LEVEL_FILTER) THEN
	  --SELECT SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000)
      --  INTO V_ERRSTACK
      --  FROM DUAL;
      --SELECT SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000)
      --  INTO V_ERROR_BACKTRACE
      --  FROM DUAL;
      V_INFO := SUBSTRB(IN_INFO, 1, 4000);
      INSERT INTO DB_LOG
        (ID,
         PROC_NAME,
         INFO,
         LOG_LEVEL,
         TIME_STAMP,
         ERROR_BACKTRACE,
         ERR_STACK,
         STEP_NO,
         LOG_DATE)
      VALUES
        (LPAD(LOG_SEQ.NEXTVAL, 20, '0'),
         IN_PROCNAME,
         V_INFO,
         IN_LEVEL,
         TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS'),
         V_ERROR_BACKTRACE,
         V_ERRSTACK,
         IN_STEPNO,
         TO_CHAR(SYSDATE, 'YYYYMMDD'));
    END IF;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE LOGGING_EXCEPTION;
  END;

  PROCEDURE DEBUG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                  IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                  IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                  ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.DEBUG_LEVEL);
  END;

  PROCEDURE INFO(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.INFO_LEVEL);
  END;

  PROCEDURE WARN(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.WARN_LEVEL);
  END;

  PROCEDURE ERROR(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.ERR_LEVEL);
  END;
END PACK_LOG;
//

CREATE OR REPLACE PACKAGE pckg_ab_export IS
  FUNCTION get_dict_value(i_dicttype IN VARCHAR2,
                          i_dictcode IN VARCHAR2,
                          i_syscode  IN VARCHAR2) RETURN VARCHAR2;
						  
  PROCEDURE proc_get_news_list(i_ssic_id    IN VARCHAR2, --统一认证号
                               i_begin_time IN VARCHAR2,
                               i_end_time   IN VARCHAR2,
                               o_ret_code   OUT VARCHAR2, --返回值
                               o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_get_post_list(i_ssic_id    IN VARCHAR2, --统一认证号
                               i_begin_time IN VARCHAR2,
                               i_end_time   IN VARCHAR2,
                               o_ret_code   OUT VARCHAR2, --返回值
                               o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_prepare_data(i_ssic_id  IN VARCHAR2,
                              o_ret_code OUT VARCHAR2, --返回值
                              o_list_cur OUT SYS_REFCURSOR --查询列表游标
                              );
  
END pckg_ab_export;
//


CREATE OR REPLACE PACKAGE BODY pckg_ab_export IS
  FUNCTION get_dict_value(i_dicttype IN VARCHAR2,
                          i_dictcode IN VARCHAR2,
                          i_syscode  IN VARCHAR2) RETURN VARCHAR2 IS
    v_parent_value ois_gen_dict.dictvalue%TYPE;
    v_value        ois_gen_dict.dictvalue%TYPE;
    v_ret          VARCHAR2(300);
  BEGIN
    SELECT MAX(d1.dictvalue)
      INTO v_parent_value
      FROM dams_gen_dict d1, dams_gen_dict d2
     WHERE d1.dicttype = d2.dicttype
       AND d1.sys_code = d2.sys_code
       AND d1.dicttype = i_dicttype
       AND d1.sys_code = i_syscode
       AND d1.dictcode = d2.parent_code
       AND d2.dictcode = i_dictcode
       AND d1.dictlang = d2.dictlang
       AND d1.dictlang = 'zh_CN';
	
	dbms_output.put_line('变量值：v_parent_value=' || v_parent_value);
	
    SELECT MAX(d.dictvalue)
      INTO v_value
      FROM dams_gen_dict d
     WHERE d.dicttype = i_dicttype
       AND d.sys_code = i_syscode
       AND d.dictcode = i_dictcode
       AND d.dictlang = 'zh_CN';
	
	dbms_output.put_line('变量值：v_value=' || v_value);
	
    IF v_parent_value IS NOT NULL AND v_parent_value <> v_value THEN
      v_ret := v_parent_value || '-' || v_value;
    ELSE
      v_ret := v_value;
    END IF;

    RETURN v_ret;
  END;
  
  PROCEDURE proc_get_news_list(i_ssic_id    IN VARCHAR2, --统一认证号
                               i_begin_time IN VARCHAR2,
                               i_end_time   IN VARCHAR2,
                               o_ret_code   OUT VARCHAR2, --返回值
                               o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_begin_time || '|' ||
                                    i_end_time;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_export.proc_get_news_list';
    v_stru_ids  VARCHAR2(1000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := '0';

    SELECT listagg(tmp.stru_id, ',') within GROUP(ORDER BY 1)
      INTO v_stru_ids
      FROM (SELECT DISTINCT bc.major_stru_id AS stru_id
              FROM dams_branch_catalog bc, dams_user_role_rel ur
             WHERE bc.stru_id = ur.stru_id
               AND ur.ssic_id = i_ssic_id
               AND ur.role_id IN ('AB000', 'AB001', 'AB002')
               AND ur.auth_state IN ('0', '1', '2')) tmp;
	
	dbms_output.put_line('变量值：v_stru_ids=' || v_stru_ids);
	
    OPEN o_list_cur FOR
      WITH stru AS
       (SELECT rpt_rsk_app_id,
               t1.dept_flag,
               listagg(t1.stru_sname, ',') within GROUP(ORDER BY t1.type, t1.sort NULLS LAST) stru_names
          FROM (SELECT t.rpt_rsk_app_id,
                       t.type,
                       t.sort,
                       b.stru_sname,
                       (SELECT COUNT(1)
                          FROM dams_branch_catalog c
                         WHERE c.stru_id = t.stru_id
                           AND c.stru_id = c.major_stru_id) dept_flag --0表示部室,1表示分行、机构
                  FROM ab_rpt_rsk_app_bnch_rel t, dams_branch b
                 WHERE t.stru_id = b.stru_id) t1
         GROUP BY rpt_rsk_app_id, t1.dept_flag)
      SELECT row_number() over(ORDER BY t1.create_time DESC) rn,
             (SELECT s.stru_names
                FROM stru s
               WHERE dept_flag = '1'
                 AND rpt_rsk_app_id = t2.rpt_rsk_app_id) fh_stru_names,
             (SELECT s.stru_names
                FROM stru s
               WHERE dept_flag = '0'
                 AND s.rpt_rsk_app_id = t2.rpt_rsk_app_id) bs_stru_names,
             nvl((SELECT d1.dictvalue
                   FROM dams_gen_dict d1, dams_gen_dict d2
                  WHERE d1.dicttype = d2.dicttype
                    AND d1.sys_code = d2.sys_code
                    AND d1.dicttype = 'AB_RISK_TYPE'
                    AND d1.sys_code = 'AB'
                    AND d1.dictcode = d2.parent_code
                    AND d2.dictcode = t2.risk_type
                    AND d1.dictlang = d2.dictlang
                    AND d1.dictlang = 'zh_CN'),
                 (SELECT d.dictvalue
                     FROM dams_gen_dict d
                    WHERE d.dicttype = 'AB_RISK_TYPE'
                      AND d.sys_code = 'AB'
                      AND d.dictcode = t2.risk_type
                      AND d.dictlang = 'zh_CN')) first_risk,
             t2.risk_type,
             (SELECT d.dictvalue
                FROM dams_gen_dict d
               WHERE d.dicttype = 'AB_RISK_TYPE'
                 AND d.sys_code = 'AB'
                 AND d.dictcode = t2.risk_type
                 AND d.dictlang = 'zh_CN') risk,
             nvl((SELECT d1.dictvalue
                   FROM dams_gen_dict d1, dams_gen_dict d2
                  WHERE d1.dicttype = d2.dicttype
                    AND d1.sys_code = d2.sys_code
                    AND d1.dicttype = 'AB_BIZ_LINE'
                    AND d1.sys_code = 'AB'
                    AND d1.dictcode = d2.parent_code
                    AND d2.dictcode = t2.biz_line
                    AND d1.dictlang = d2.dictlang
                    AND d1.dictlang = 'zh_CN'),
                 (SELECT d.dictvalue
                     FROM dams_gen_dict d
                    WHERE d.dicttype = 'AB_BIZ_LINE'
                      AND d.sys_code = 'AB'
                      AND d.dictcode = t2.biz_line
                      AND d.dictlang = 'zh_CN')) first_biz,
             t2.biz_line,
             (SELECT d.dictvalue
                FROM dams_gen_dict d
               WHERE d.dicttype = 'AB_BIZ_LINE'
                 AND d.sys_code = 'AB'
                 AND d.dictcode = t2.biz_line
                 AND d.dictlang = 'zh_CN') biz,
             t2.event_title,
             to_char(t2.event_time, 'YYYY') || '年' ||
             to_char(t2.event_time, 'MM') || '月' ||
             to_char(t2.event_time, 'DD') || '日' event_time,
             t2.news_media,
             t2.news_republish_and_link,
             decode(t2.serious_degree, '1', '特别重大', '2', '重大', '3', '一般', '') serious_degree,
             CASE
               WHEN EXISTS (SELECT 1
                       FROM ab_rpt_rsk_app_wf aw, dams_wf_process p
                      WHERE p.business_id = aw.business_id
                        AND aw.rpt_rsk_app_id = t2.rpt_rsk_app_id
                        AND aw.source_business_id IS NULL) THEN
                '否'
               ELSE
                '是'
             END app_status, --是否归档
             t1.appraise_content,
             t1.appraise_event_strus,
             decode(t1.appraise_is_solved, '1', '是', '2', '否', '3', '正在处置', '否') appraise_is_solved,
             decode(t1.appraise_has_improvement, '1', '是', '0', '否', '否') appraise_has_improvement,
             decode(t1.appraise_solution, '1', '撤稿/删帖', '2', '隐名/更正', '3',
                    '评论/跟贴引导', '4', '准备口径', '5', '发布口径', '6', '正面报道') appraise_solution,
             decode(t1.appraise_remind_situation, '1', '系统提示', '2', '邮件提示', '3',
                    '微信提示', '4', '电话提示') appraise_remind_situation,
             decode(t1.appraise_is_report_cbrc, '1', '是', '0', '否', '否') appraise_is_report_cbrc,
             t1.appraise_spread_situation,
             t1.appraise_remarks,
             decode(t1.appraise_feedback_situation, '1', '系统反馈', '2', '邮件反馈',
                    '3', '微信反馈', '4', '电话反馈') appraise_feedback_situation,
             pckg_ab_export.get_dict_value('AB_IS_FOREIGN_RELATED',
                                           t2.is_foreign_related, 'AB') is_foreign_related
        FROM ab_rpt_rsk_app_wf t1,
             ab_rpt_rsk_app t2,
             (SELECT MAX(aaw.business_id) keep(dense_rank FIRST ORDER BY aaw.create_time DESC) AS business_id
                FROM ab_rpt_rsk_app t, ab_rpt_rsk_app_wf aaw
               WHERE aaw.rpt_rsk_app_id = t.rpt_rsk_app_id
                 AND t.rpt_rsk_app_type = '1'
                 AND ((instr(v_stru_ids, '0010100000') > 0 AND
                     aaw.source_business_id IS NULL) OR
                     instr(v_stru_ids, aaw.stru_id) > 0)
                 AND aaw.status = '1'
                 AND (i_begin_time IS NULL OR
                     aaw.create_time >= to_date(i_begin_time, 'yyyymmdd'))
                 AND (i_end_time IS NULL OR
                     aaw.create_time <= to_date(i_end_time, 'yyyymmdd'))
               GROUP BY aaw.rpt_rsk_app_id) t3
       WHERE t1.rpt_rsk_app_id = t2.rpt_rsk_app_id
         AND t1.business_id = t3.business_id;
		
  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_news_list;
  
  PROCEDURE proc_get_post_list(i_ssic_id    IN VARCHAR2, --统一认证号
                               i_begin_time IN VARCHAR2,
                               i_end_time   IN VARCHAR2,
                               o_ret_code   OUT VARCHAR2, --返回值
                               o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_begin_time || '|' ||
                                    i_end_time;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_export.proc_get_post_list';
    v_stru_ids  VARCHAR2(1000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := '0';

    SELECT listagg(tmp.stru_id, ',') within GROUP(ORDER BY 1)
      INTO v_stru_ids
      FROM (SELECT DISTINCT bc.major_stru_id AS stru_id
              FROM dams_branch_catalog bc, dams_user_role_rel ur
             WHERE bc.stru_id = ur.stru_id
               AND ur.ssic_id = i_ssic_id
               AND ur.role_id IN ('AB000', 'AB001', 'AB002')
               AND ur.auth_state IN ('0', '1', '2')) tmp;
	
	dbms_output.put_line('变量值：v_stru_ids='||v_stru_ids);
	
    OPEN o_list_cur FOR
      WITH stru AS
       (SELECT rpt_rsk_app_id,
               t1.dept_flag,
               listagg(t1.stru_sname, ',') within GROUP(ORDER BY t1.type, t1.sort NULLS LAST) stru_names
          FROM (SELECT t.rpt_rsk_app_id,
                       t.type,
                       t.sort,
                       b.stru_sname,
                       (SELECT COUNT(1)
                          FROM dams_branch_catalog c
                         WHERE c.stru_id = t.stru_id
                           AND c.stru_id = c.major_stru_id) dept_flag
                  FROM ab_rpt_rsk_app_bnch_rel t, dams_branch b
                 WHERE t.stru_id = b.stru_id) t1
         GROUP BY rpt_rsk_app_id, t1.dept_flag)
      SELECT row_number() over(ORDER BY t1.create_time DESC) rn,
             (SELECT s.stru_names
                FROM stru s
               WHERE dept_flag = '1'
                 AND rpt_rsk_app_id = t2.rpt_rsk_app_id) fh_stru_names,
             (SELECT s.stru_names
                FROM stru s
               WHERE dept_flag = '0'
                 AND s.rpt_rsk_app_id = t2.rpt_rsk_app_id) bs_stru_names,
             nvl((SELECT d1.dictvalue
                   FROM dams_gen_dict d1, dams_gen_dict d2
                  WHERE d1.dicttype = d2.dicttype
                    AND d1.sys_code = d2.sys_code
                    AND d1.dicttype = 'AB_RISK_TYPE'
                    AND d1.sys_code = 'AB'
                    AND d1.dictcode = d2.parent_code
                    AND d2.dictcode = t2.risk_type
                    AND d1.dictlang = d2.dictlang
                    AND d1.dictlang = 'zh_CN'),
                 (SELECT d.dictvalue
                     FROM dams_gen_dict d
                    WHERE d.dicttype = 'AB_RISK_TYPE'
                      AND d.sys_code = 'AB'
                      AND d.dictcode = t2.risk_type
                      AND d.dictlang = 'zh_CN')) first_risk,
             t2.risk_type,
             (SELECT d.dictvalue
                FROM dams_gen_dict d
               WHERE d.dicttype = 'AB_RISK_TYPE'
                 AND d.sys_code = 'AB'
                 AND d.dictcode = t2.risk_type
                 AND d.dictlang = 'zh_CN') risk,
             nvl((SELECT d1.dictvalue
                   FROM dams_gen_dict d1, dams_gen_dict d2
                  WHERE d1.dicttype = d2.dicttype
                    AND d1.sys_code = d2.sys_code
                    AND d1.dicttype = 'AB_BIZ_LINE'
                    AND d1.sys_code = 'AB'
                    AND d1.dictcode = d2.parent_code
                    AND d2.dictcode = t2.biz_line
                    AND d1.dictlang = d2.dictlang
                    AND d1.dictlang = 'zh_CN'),
                 (SELECT d.dictvalue
                     FROM dams_gen_dict d
                    WHERE d.dicttype = 'AB_BIZ_LINE'
                      AND d.sys_code = 'AB'
                      AND d.dictcode = t2.biz_line
                      AND d.dictlang = 'zh_CN')) first_biz,
             t2.biz_line,
             (SELECT d.dictvalue
                FROM dams_gen_dict d
               WHERE d.dicttype = 'AB_BIZ_LINE'
                 AND d.sys_code = 'AB'
                 AND d.dictcode = t2.biz_line
                 AND d.dictlang = 'zh_CN') biz,
             t2.event_title,
             to_char(t2.event_time, 'YYYY') || '年' ||
             to_char(t2.event_time, 'MM') || '月' ||
             to_char(t2.event_time, 'DD') || '日' event_time,
             t2.event_content,
             t2.post_carrier,
             t2.post_link,
             decode(t2.serious_degree, '1', '特别重大', '2', '重大', '3', '一般', '') serious_degree,
             CASE
               WHEN EXISTS (SELECT 1
                       FROM ab_rpt_rsk_app_wf aw, dams_wf_process p
                      WHERE p.business_id = aw.business_id
                        AND aw.rpt_rsk_app_id = t2.rpt_rsk_app_id
                        AND aw.source_business_id IS NULL) THEN
                '否'
               ELSE
                '是'
             END app_status, --是否归档
             t1.appraise_content,
             t1.appraise_event_strus,
             decode(t1.appraise_is_solved, '1', '是', '2', '否', '3', '正在处置', '否') appraise_is_solved,
             decode(t1.appraise_has_improvement, '1', '是', '0', '否', '否') appraise_has_improvement,
             decode(t1.appraise_solution, '1', '撤稿/删帖', '2', '隐名/更正', '3',
                    '评论/跟贴引导', '4', '准备口径', '5', '发布口径', '6', '正面报道') appraise_solution,
             decode(t1.appraise_remind_situation, '1', '系统提示', '2', '邮件提示', '3',
                    '微信提示', '4', '电话提示') appraise_remind_situation,
             decode(t1.appraise_is_report_cbrc, '1', '是', '0', '否', '否') appraise_is_report_cbrc,
             t1.appraise_spread_situation,
             t1.appraise_remarks,
             decode(t1.appraise_feedback_situation, '1', '系统反馈', '2', '邮件反馈',
                    '3', '微信反馈', '4', '电话反馈') appraise_feedback_situation,
             pckg_ab_export.get_dict_value('AB_IS_FOREIGN_RELATED',
                                           t2.is_foreign_related, 'AB') is_foreign_related
        FROM ab_rpt_rsk_app_wf t1,
             ab_rpt_rsk_app t2,
             (SELECT MAX(aaw.business_id) keep(dense_rank FIRST ORDER BY aaw.create_time DESC) AS business_id
                FROM ab_rpt_rsk_app t, ab_rpt_rsk_app_wf aaw
               WHERE aaw.rpt_rsk_app_id = t.rpt_rsk_app_id
                 AND t.rpt_rsk_app_type = '2'
                 AND ((instr(v_stru_ids, '0010100000') > 0 AND
                     aaw.source_business_id IS NULL) OR
                     instr(v_stru_ids, aaw.stru_id) > 0)
                 AND aaw.status = '1'
                 AND (i_begin_time IS NULL OR
                     aaw.create_time >= to_date(i_begin_time, 'yyyymmdd'))
                 AND (i_end_time IS NULL OR
                     aaw.create_time <= to_date(i_end_time, 'yyyymmdd'))
               GROUP BY aaw.rpt_rsk_app_id) t3
       WHERE t1.rpt_rsk_app_id = t2.rpt_rsk_app_id
         AND t1.business_id = t3.business_id;
  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_post_list;
  
  PROCEDURE proc_prepare_data(i_ssic_id  IN VARCHAR2,
                              o_ret_code OUT VARCHAR2, --返回值
                              o_list_cur OUT SYS_REFCURSOR --查询列表游标
                              ) IS
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_export.proc_prepare_data';

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    OPEN o_list_cur FOR
      SELECT b.stru_id, b.stru_sname
        FROM dams_branch b,
             (SELECT t.stru_id
                FROM dams_branch t,
                     (SELECT DISTINCT bc.major_stru_id
                        FROM dams_user_role_rel ur, dams_branch_catalog bc
                       WHERE bc.stru_id = ur.stru_id
                         AND ur.role_id LIKE 'AB%'
                         AND ur.ssic_id = i_ssic_id) t1
               WHERE t.sup_stru = t1.major_stru_id
                 AND t.stru_state IN ('1', '2')
                 AND t.stru_sign <> '2'
              UNION
              SELECT t.stru_id
                FROM dams_branch t,
                     dams_branch_catalog bc,
                     (SELECT DISTINCT bc.major_stru_id
                        FROM dams_user_role_rel ur, dams_branch_catalog bc
                       WHERE bc.stru_id = ur.stru_id
                         AND ur.role_id LIKE 'AB%'
                         AND ur.ssic_id = i_ssic_id) t1
               WHERE t.stru_id = bc.dept_stru_id
                 AND bc.major_stru_id = t1.major_stru_id
                 AND t.stru_state IN ('1', '2')) tmp
       WHERE b.stru_id = tmp.stru_id
       ORDER BY b.man_grade,
                b.stru_sort DESC NULLS LAST,
                b.sort DESC NULLS LAST,
                b.stru_sname;
  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_prepare_data;
  
END pckg_ab_export;
//


CREATE OR REPLACE package pckg_ctp_lg_public is
  PROCEDURE LOG(proc_name IN VARCHAR2, info IN VARCHAR2);
end pckg_ctp_lg_public;
//


CREATE OR REPLACE package body pckg_ctp_lg_public is
  PROCEDURE log(proc_name IN VARCHAR2, info IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    time_str VARCHAR2(100);
  BEGIN
    --SELECT to_char(SYSDATE, 'mm - dd - yyyy hh24 :mi :ss') INTO time_str FROM dual;
    SELECT to_char(to_date('20210119','yyyymmdd'), 'mm - dd - yyyy hh24 :mi :ss') INTO time_str FROM dual;
    INSERT INTO ctp_proc_log VALUES (proc_name, time_str, info);
    COMMIT;
    RETURN;
  END;
end pckg_ctp_lg_public;
//


CREATE OR REPLACE PACKAGE pckg_dams_util IS
  TYPE ref_cur IS REF CURSOR;
  TYPE r_sparea_list IS RECORD(
    sp_area      sf_card_sp_area_def.sp_area%TYPE,
    sp_area_name sf_card_sp_area_def.sp_area_name%TYPE);
  TYPE ref_sparea_list IS REF CURSOR RETURN r_sparea_list;
  
  FUNCTION func_decode(i_type   IN VARCHAR2, --字典类型，若类型后加_R，则为中文转代码
                       i_code   IN VARCHAR2, --字典代码
                       i_system IN VARCHAR2, --子系统编码
                       i_lang   IN VARCHAR2 := 'zh_CN' --语言代码
                       ) RETURN VARCHAR2;
  
  FUNCTION func_gen_seq(i_prefix IN VARCHAR2, --主键前缀
                        i_seq    IN VARCHAR2, --序列器名称
                        i_len    IN NUMBER := 7 --序列器填充长度
                        ) RETURN VARCHAR2;
  
  FUNCTION func_str_to_array(i_str        IN VARCHAR2, --需分割的字符串
                             i_split_str  IN VARCHAR2, --分隔符
                             i_split_flag IN VARCHAR2 := 1 --拆分方式，为1时,1;2拆分为[1,2]，其他拆分为[1]
                             ) RETURN dams_arrytype;
  
  FUNCTION func_getusernamebyid(i_ssic_id IN VARCHAR2 --人员编码
                                ) RETURN VARCHAR2;
  
  PROCEDURE proc_get_user_info(i_user_name IN VARCHAR2, --输入用户名
                               i_stru_id   IN VARCHAR2, --限制机构范围
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_list      OUT SYS_REFCURSOR --返回用户信息
                               );
  
  PROCEDURE proc_query_stru_user(i_stru_id  IN VARCHAR2,
                                 i_qname    IN VARCHAR2,
                                 i_lang     IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR);
  
  PROCEDURE proc_get_stru_info(i_stru_name IN VARCHAR2, --输入部门信息
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_list      OUT SYS_REFCURSOR --返回用户信息
                               );
  
  PROCEDURE proc_reset_natural_holiday(i_bgn_date IN VARCHAR2, --开始日期
                                       i_end_date IN VARCHAR2, --结束日期
                                       o_ret_code OUT VARCHAR2 --返回值
                                       );
  
  PROCEDURE proc_set_natural_holiday(i_bgn_date   IN VARCHAR2, --开始日期
                                     i_end_date   IN VARCHAR2, --结束日期
                                     i_is_holiday IN VARCHAR2, --是否节假日 1-是，2-否
                                     o_ret_code   OUT VARCHAR2 --返回值
                                     );
  
  PROCEDURE proc_cacu_day_span(i_include_holiday IN VARCHAR2, --是否包含节假日 1-包含 0-不包含
                               i_bgn_date        IN VARCHAR2, --开始日期
                               i_bgn_time_type   IN VARCHAR2, --开始时间类型   1-上午 2-下午
                               i_end_date        IN VARCHAR2, --结束日期
                               i_end_time_type   IN VARCHAR2, --结束时间类型   1-上午 2-下午
                               o_ret_code        OUT VARCHAR2, --返回值
                               o_day_num         OUT VARCHAR2);
  
  PROCEDURE proc_branch_tree(i_id          IN VARCHAR2, --机构编码
                             i_rawid       IN VARCHAR2, --原始节点编码，判断根节点
                             i_system      IN VARCHAR2, --系统编码
                             o_flag        OUT VARCHAR2, --存储过程返回标志
                             o_branch_list OUT SYS_REFCURSOR --角色列表
                             );
  
  PROCEDURE proc_get_stru_name(i_stru_id   IN VARCHAR2, --机构编码
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_stru_name OUT VARCHAR2 --机构路径
                               );
  
  FUNCTION func_get_stru_name(i_stru_id IN VARCHAR2 --机构编码
                              ) RETURN VARCHAR2;
  
  PROCEDURE proc_get_stru_name_bat(i_stru_ids IN VARCHAR2, --机构编码
                                   o_ret_code OUT VARCHAR2, --返回信息
                                   o_list     OUT SYS_REFCURSOR --机构路径
                                   );
  
  PROCEDURE proc_get_stru_path(i_stru_id   IN VARCHAR2, --机构编码
                               i_sep       IN VARCHAR2 := '/', --分隔符
                               i_flag      IN VARCHAR2 := 0, --0,向上取至一级企业
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_stru_name OUT VARCHAR2 --机构路径
                               );
  
  FUNCTION func_get_stru_path(i_stru_id IN VARCHAR2, --机构编码
                              i_sep     IN VARCHAR2 := '/', --分隔符
                              i_flag    IN VARCHAR2 := 0 --0,向上取至一级企业
                              ) RETURN VARCHAR2;
  
  PROCEDURE proc_process_close(o_retcode OUT VARCHAR2,
                               o_msg     OUT VARCHAR2);
  
  PROCEDURE proc_dams_logger(i_logobj     IN VARCHAR2, --操作对象
                             i_logtype    IN VARCHAR2, --操作对象下的子类型
                             i_logdate    IN VARCHAR2, --操作日期
                             i_loghandler IN VARCHAR2, --操作人
                             i_loginfo    IN VARCHAR2, --概要信息
                             i_logdetail  IN CLOB := '' --详细信息（非必输）
                             );
  
  FUNCTION func_get_deptlevel1(i_stru_id IN VARCHAR2, --机构编码
                               i_type    IN VARCHAR2 := '0' --0返回机构编码 1返回机构名称
                               ) RETURN VARCHAR2;
  
  PROCEDURE proc_get_deptlevel1(i_stru_id  IN VARCHAR2, --输入机构
                                o_ret_code OUT VARCHAR2,
                                o_msg      OUT VARCHAR2,
                                o_list     OUT SYS_REFCURSOR);

  --PROCEDURE proc_get_st_pdkey(i_stru_id  IN VARCHAR2, --机构
  --                            i_sec_lv   IN VARCHAR2, --密级
  --                            i_dsm_type IN VARCHAR2, --涉密文档提供类型：1复制 2对外提供
  --                            o_ret_code OUT VARCHAR2,
  --                            o_msg      OUT VARCHAR2,
  --                            o_pdkey    OUT VARCHAR2,
  --                            o_biztype  OUT VARCHAR2);
  
  FUNCTION func_get_lv1_branch(i_stru_id IN VARCHAR2 --机构编码
                               ) RETURN VARCHAR2;

  FUNCTION func_get_lv2_branch(i_stru_id IN VARCHAR2 --机构编码
                               ) RETURN VARCHAR2;
  
  PROCEDURE proc_get_lv1_branch(i_stru_id    IN VARCHAR2, --机构编码
                                o_ret_code   OUT VARCHAR2, --返回信息
                                o_lv1_branch OUT VARCHAR2 --返回用户信息
                                );
  
  PROCEDURE proc_get_lv2_branch(i_stru_id    IN VARCHAR2, --机构编码
                                o_ret_code   OUT VARCHAR2, --返回信息
                                o_lv2_branch OUT VARCHAR2 --返回用户信息
                                );
  
  PROCEDURE proc_user_choice_list(i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  i_branch_id IN VARCHAR2, --机构编码
                                  i_name      IN VARCHAR2, --
                                  i_sys_code  IN VARCHAR2,
                                  o_ret_code  OUT VARCHAR2, --存储过程返回标志
                                  o_msg       OUT VARCHAR2,
                                  o_total_num OUT VARCHAR2,
                                  o_user_list OUT SYS_REFCURSOR);
  
  PROCEDURE proc_get_role_strus(i_ssic_id  IN VARCHAR2, --用户编码
                                i_role_id  IN VARCHAR2, --角色id
                                o_ret_code OUT VARCHAR2, --返回标志
                                o_msg      OUT VARCHAR2, --返回信息
                                o_count    OUT VARCHAR2, --机构数量
                                o_strus    OUT SYS_REFCURSOR --机构列表
                                );
  
  PROCEDURE proc_get_role_menu(i_ssic_id  IN VARCHAR2,
                               o_ret_code OUT VARCHAR2,
                               o_menus    OUT VARCHAR2);
  
  PROCEDURE proc_get_dept_stru(i_stru_id   IN VARCHAR2,
                               o_ret_code  OUT VARCHAR2,
                               o_dept_stru OUT SYS_REFCURSOR);
  
  FUNCTION func_getregionno60(in_year_month IN VARCHAR2) RETURN VARCHAR2;
  
  --FUNCTION encrypt_3des(clear_text IN VARCHAR2, --需加密的明文
  --                      key        IN VARCHAR2 --密钥
  --                      ) RETURN VARCHAR2;
  --
  --FUNCTION decrypt_3des(cipher_text IN VARCHAR2, --需加密的明文
  --                      key         IN VARCHAR2 --密钥
  --                      ) RETURN VARCHAR2;
	--					
  --FUNCTION des(i_sign IN VARCHAR2, --秘钥标识
  --             i_text IN VARCHAR2 --
  --             ) RETURN VARCHAR2;
  --
  --FUNCTION getkey(i_sign IN VARCHAR2 --秘钥标识
  --                ) RETURN VARCHAR2;
  --
  --FUNCTION getkey(i_sign     IN VARCHAR2, --秘钥标识
  --                i_dictcode IN VARCHAR2,
  --                i_sys_code IN VARCHAR2) RETURN VARCHAR2;  


END pckg_dams_util;
//


CREATE OR REPLACE PACKAGE BODY pckg_dams_util IS
	--"$hits" sys.aaa_coverage_tool.bool_tab; 
	--"$times" sys.aaa_coverage_tool.bool_tab;
	--procedure "$log"(x integer) is
	--  procedure log(id integer, stm_no integer) is
	--	pragma autonomous_transaction;
	--  begin
	--	update sys.aaa_coverage_statements c
	--	   set hit = 1
	--	 where hit = 0
	--	   and cvr_id = log.id and stm_no = log.stm_no;
	--	commit;
	--  end;
	--  procedure logtime(id integer, stm_no integer) is
	--	pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	--  begin
	--	insert into sys.aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit; 
        --end;
	--  begin
	--   if "$hits" is null then 
	--	"$hits" := sys.aaa_coverage_tool.get_statement_tab(5);
	--   end if;
	--   if "$times" is null then 
	--	"$times" := sys.aaa_coverage_tool.get_statement_time_tab(5);
	--   end if;
	--   if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
	--	log(5,x); "$hits"(x):=true; 
	--   end if;
	--   if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
	--	logtime(5,x); "$times"(x):=true; 
	--   end if;
	--end;
  
  FUNCTION func_decode(i_type   IN VARCHAR2, --字典类型，若类型后加_R，则为中文转代码
                       i_code   IN VARCHAR2, --字典代码
                       i_system IN VARCHAR2, --子系统编码
                       i_lang   IN VARCHAR2 := 'zh_CN' --语言代码
                       ) RETURN VARCHAR2 IS
    v_result dams_gen_dict.dictvalue%TYPE;
    v_type   dams_gen_dict.dicttype%TYPE := upper(TRIM(i_type));
    v_count  NUMBER := NULL;
  BEGIN
    IF substr(v_type, lengthb(v_type) - 1, 2) = '_R' THEN
      --反解码  中文转代码
      SELECT COUNT(1)
        INTO v_count
        FROM dams_gen_dict
       WHERE dicttype = substr(v_type, 1, lengthb(v_type) - 2)
         AND sys_code = i_system
         AND dictlang = i_lang;   
      IF v_count = 0 THEN
        RETURN i_code;
      END IF;  
      SELECT dictcode
        INTO v_result
        FROM dams_gen_dict
       WHERE dicttype = substr(v_type, 1, lengthb(v_type) - 2)
         AND sys_code = i_system
         AND dictlang = i_lang
         AND dictvalue = i_code; 
    ELSE
      --解码  代码转中文
      SELECT MAX(dictvalue)
        INTO v_result
        FROM dams_gen_dict
       WHERE dicttype = v_type
         AND sys_code = i_system
         AND dictlang = i_lang
         AND dictcode = i_code; 
      IF v_result IS NULL THEN
        RETURN i_code;
      END IF; 
    END IF;
    RETURN v_result;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END func_decode;
  
  FUNCTION func_gen_seq(i_prefix IN VARCHAR2, --主键前缀
                        i_seq    IN VARCHAR2, --序列器名称
                        i_len    IN NUMBER := 7 --序列器填充长度
                        ) RETURN VARCHAR2 IS
    --v_date   VARCHAR2(9) := to_char(SYSDATE, 'YYYY');
    v_date   VARCHAR2(9) := to_char(TO_DATE('20210119','yyyymmdd'), 'YYYY');
    v_result VARCHAR2(30);
    v_sql    VARCHAR2(4000);
    v_seqval VARCHAR2(20);
    v_prefix VARCHAR2(20);
    v_len    NUMBER;
  BEGIN
    v_sql := 'SELECT ' || i_seq || '.NEXTVAL FROM DUAL';
    v_len := i_len;
    EXECUTE IMMEDIATE v_sql INTO v_seqval;
    v_prefix := i_prefix;
    IF i_prefix = 'SA' THEN
      v_prefix := 'XXAQ';
    ELSIF i_prefix = 'SD' THEN
      v_prefix := 'BGYP';
    ELSIF i_prefix = 'SE' THEN
      v_prefix := 'WJYS';
    ELSIF i_prefix = 'SF' THEN
      v_prefix := 'CRZK';
    ELSIF i_prefix = 'SG' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SH' THEN
      v_prefix := 'GWCK';
    ELSIF i_prefix = 'SI' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SJ' THEN
      v_prefix := 'ZFWJ';
    ELSIF i_prefix = 'SK' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SL' THEN
      v_prefix := 'DAJY';
    ELSIF i_prefix = 'SN' THEN
      --技术维护服务
      v_prefix := 'JSWH';
    ELSIF i_prefix = 'SO' THEN
      --设备管理
      v_prefix := 'SBSL';
    ELSIF i_prefix = 'SP' THEN
      --耗材申领管理---hcsl
      v_prefix := 'HCSL';
      --kfzx-guoq03 added
    ELSIF i_prefix = 'SR' THEN
      --参数审批管理
      v_prefix := 'CSGL';
    ELSIF i_prefix = 'SS' THEN
      --办公电话
      v_prefix := 'BGDH';
    ELSIF i_prefix = 'ST' THEN
      --保密系统
      v_prefix := 'BMXT';
    ELSIF i_prefix = 'STI' THEN
      --保密信息
      v_prefix := 'BMXX';
      v_date   := to_char(TO_DATE('20210120','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SUO' THEN
      --加班申请
      v_prefix := 'JBSQ';
      v_date   := to_char(TO_DATE('20210121','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SU' THEN
      --请假申请
      v_prefix := 'QJSQ';
      v_date   := to_char(TO_DATE('20210122','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'XJSQ' THEN
      --销假申请
      v_prefix := 'XJSQ';
      v_date   := to_char(TO_DATE('20210123','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'CCSQ' THEN
      --出差申请
      v_prefix := 'CCSQ';
      v_date   := to_char(TO_DATE('20210124','yyyymmdd'), 'YYYYMMDD');
       v_date   := v_date || '0';
       v_len    := v_len - 1;
    ELSIF i_prefix = 'SW' THEN
      --合同申请
      v_prefix := 'HTSQ';
      v_date   := to_char(TO_DATE('20210125','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SWI' THEN
      --合同管理
      v_prefix := 'HTGL';
      v_date   := to_char(TO_DATE('20210126','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'BXSQ' THEN
      --报销申请
      v_prefix := 'BXSQ';
      v_date   := to_char(TO_DATE('20210127','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'CCBX' THEN
      --出差报销
      v_prefix := 'CCBX';
      v_date   := to_char(TO_DATE('20210128','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'CCBL' THEN
      --出差报销补录
      v_prefix := 'CCBL';
      v_date   := to_char(TO_DATE('20210129','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'JKSQ' THEN
      --借款申请
      v_prefix := 'JKSQ';
      v_date   := to_char(TO_DATE('20210130','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'LX' THEN
      --信访事项
      v_prefix := 'LX';
      v_date   := to_char(TO_DATE('20210201','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'LF' THEN
      --信访事项
      v_prefix := 'LF';
      v_date   := to_char(TO_DATE('20210202','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'LD' THEN
      --信访事项
      v_prefix := 'LD';
      v_date   := to_char(TO_DATE('20210203','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'JA' THEN
      --信访积案
      v_prefix := 'JA';
      v_date   := to_char(TO_DATE('20210204','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'DB' THEN
      --信访督办
      v_prefix := 'DB';
      v_date   := to_char(TO_DATE('20210205','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SZ' THEN
      --移动办公开销户申请
      v_prefix := 'YDSQ';
      v_date   := to_char(TO_DATE('20210206','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'AA' THEN
      --法审
      v_prefix := 'FLSC';
      v_date   := to_char(TO_DATE('20210207','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'XFHT' THEN
      --信访印章被退回历史
      v_prefix := 'XFHT';
      v_date   := to_char(TO_DATE('20210208','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'AB' THEN
      --声誉风险
      v_prefix := 'SYFX';
      v_date   := to_char(TO_DATE('20210209','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'YQ' THEN
      --声誉风险 舆情单号
      v_prefix := 'YQ';
      v_date   := to_char(TO_DATE('20210210','yyyymmdd'), 'YYYY');
    ELSIF i_prefix = 'FXPC' THEN
      --声誉风险 风险排查
      v_prefix := 'FXPC';
      v_date   := to_char(TO_DATE('20210211','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'TZSQ' THEN
      --声誉风险 风险排查申请
      v_prefix := 'TZSQ';
      v_date   := to_char(TO_DATE('20210212','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'PCDB' THEN
      --声誉风险 风险排查申请待办
      v_prefix := 'PCDB';
      v_date   := to_char(TO_DATE('20210213','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'YJYL' THEN
      --声誉风险 应急演练
      v_prefix := 'YJYL';
      v_date   := to_char(TO_DATE('20210214','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SYTZ' THEN
      --声誉风险 通知
      v_prefix := 'SYTZ';
      v_date   := to_char(TO_DATE('20210215','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SYBG' THEN
      --声誉风险 报告
      v_prefix := 'SYBG';
      v_date   := to_char(TO_DATE('20210216','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'GP' THEN
      --群组号
      v_prefix := 'GP';
      v_date   := to_char(TO_DATE('20210217','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SXOU' THEN
      --财务 系统外人员维护申请
      v_prefix := 'SXOU';
      v_date   := to_char(TO_DATE('20210218','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'SXCP' THEN
      --财务 跨部室项目维护申请
      v_prefix := 'SXCP';
      v_date   := to_char(TO_DATE('20210219','yyyymmdd'), 'YYYYMMDD');
    ELSIF i_prefix = 'PROJECT' THEN
      --财务 跨部室项目编号
      v_prefix := 'PROJECT';
      v_date   := to_char(TO_DATE('20210220','yyyymmdd'), 'YYYYMMDD');
    END IF;
    v_result := v_prefix || v_date || lpad(v_seqval, v_len, '0');
    RETURN v_result;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END func_gen_seq;
  
  FUNCTION func_str_to_array(i_str        IN VARCHAR2, --需分割的字符串
                             i_split_str  IN VARCHAR2, --分隔符
                             i_split_flag IN VARCHAR2 := 1 --拆分方式，为1时,1;2拆分为[1,2]，其他拆分为[1]
                             ) RETURN dams_arrytype IS
    v_count1  INTEGER;
    v_count2  INTEGER;
    v_strlist dams_arrytype;
    v_node    VARCHAR2(4000);
    v_len     INTEGER;
  BEGIN
    v_count2  := 0;
    v_strlist := dams_arrytype();
    v_len     := lengthb(i_split_str);
    IF (i_str IS NULL) OR (length(i_str) <= 0) THEN
      RETURN NULL;
    END IF;
    FOR v_i IN 0 .. length(i_str)
    LOOP
      IF v_i = 0 THEN
        v_count1 := 1 - v_len; --第一个字符段
        v_count2 := instrb(i_str, i_split_str, 1, v_i + 1);
      ELSE
        v_count1 := instrb(i_str, i_split_str, 1, v_i);
        v_count2 := instrb(i_str, i_split_str, 1, v_i + 1);
      END IF;
      IF v_count2 = 0 THEN
        --若后面无分隔符，则判断标志是否添加剩余字符串
        IF i_split_flag = '1' AND v_count1 < lengthb(i_str) THEN
          v_node := substrb(i_str, v_count1 + v_len);
        ELSE
          EXIT;
        END IF;
      ELSE
        v_node := substrb(i_str, v_count1 + v_len, v_count2 - v_count1 - v_len);
      END IF;
      IF v_node IS NULL THEN
        v_node := '';
      END IF;
      v_strlist.extend();
      v_strlist(v_i + 1) := v_node;
      v_node := '';
      --若后面无分隔符，则退出循环
      IF v_count2 = 0 AND i_split_flag = '1' THEN
        EXIT;
      END IF; 
    END LOOP;
    RETURN v_strlist;
  END func_str_to_array;
  
  FUNCTION func_getusernamebyid(i_ssic_id IN VARCHAR2 --人员编码
                                ) RETURN VARCHAR2 IS
    v_name dams_user.name%TYPE;
  BEGIN
    SELECT MAX(t.name)
      INTO v_name
      FROM dams_user t
     WHERE t.ssic_id = i_ssic_id;
  
    RETURN v_name;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END func_getusernamebyid;
  
  PROCEDURE proc_get_user_info(i_user_name IN VARCHAR2, --输入用户名
                               i_stru_id   IN VARCHAR2, --限制机构范围
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_list      OUT SYS_REFCURSOR --返回用户信息
                               ) IS
    v_param     db_log.info%TYPE := i_user_name || '|' || i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_common.proc_get_user_info';
    v_step      db_log.step_no%TYPE;
  BEGIN
    o_ret_code := 0;
    OPEN o_list FOR  
      SELECT u.name || '|' || s.stru_sname || '|' || s.stru_id || '|' || n.name || '|' ||
             n.name text,
             u.ssic_id VALUE
        FROM dams_branch s, dams_user u, dams_prm_user_nls n
       WHERE u.name LIKE '%' || i_user_name || '%'
         AND u.stru_id = s.stru_id
         AND s.stru_id = nvl(i_stru_id, s.stru_id)
         AND n.code(+) = u.admin_duty
         AND n.dict_kind(+) = 'ADMIN_DUTY'
         AND n.locale(+) = 'zh_CN'
         AND rownum < 50;
    --GROUP BY u.ssic_id, u.NAME;
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
      IF o_list%ISOPEN THEN
        CLOSE o_list;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_user_info;
  
  PROCEDURE proc_query_stru_user(i_stru_id  IN VARCHAR2,
                                 i_qname    IN VARCHAR2,
                                 i_lang     IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_stru_id || '|' || i_qname || '|' ||
                                     i_lang;
    v_arrall     dams_arrytype;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_UTIL.proc_query_stru_user';
    v_tmp_count  NUMBER(5);
    v_lv2_branch VARCHAR2(10);
    v_stru_id    VARCHAR2(10);
  BEGIN
    o_ret_code := '0';
    --单值的情况，优化为如传入分支机构且有机构本部，则只找机构本部下的人员
    IF NOT (instr(i_stru_id, ';') > 0) THEN
      SELECT COUNT(1)
        INTO v_tmp_count
        FROM dams_branch b
       WHERE i_stru_id = b.sup_stru
         AND b.stru_sign = '2'
         AND b.stru_state IN ('1', '2');  
	  dbms_output.put_line('v_tmp_count='||v_tmp_count);
      IF v_tmp_count > 0 THEN
        SELECT b.stru_id
          INTO v_lv2_branch
          FROM dams_branch b
         WHERE instr(i_stru_id, b.sup_stru) > 0
           AND b.stru_sign = '2'
           AND b.stru_state IN ('1', '2')
           AND rownum = 1;
        v_stru_id := v_lv2_branch;
		dbms_output.put_line('v_stru_id='||v_stru_id);
      ELSE
        v_stru_id := i_stru_id;
      END IF;   
      OPEN o_list FOR
        SELECT text, ssic_id
          FROM (SELECT a.name || '|' || a.stru_sname || '|' || a.stru_id || '|' ||
                       nvl(n.name, n1.name) || '|' || a.office_phone1 text,
                       a.ssic_id,
                       row_number() over(ORDER BY a.duty_sort DESC NULLS LAST, nvl(a.if_preside_work, '502002') ASC, a.sort DESC NULLS LAST, a.name ASC NULLS LAST) rn
                  FROM (SELECT u.name,
                               b.stru_sname,
                               b.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               u.if_preside_work,
                               u.office_phone1,
                               u.sort
                          FROM dams_user u, dams_branch b
                         WHERE u.stru_id = i_stru_id
                           AND u.stru_id = b.stru_id
                           AND v_tmp_count > 0
                        UNION ALL
                        SELECT u.name,
                               b.stru_sname,
                               b.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               ur.if_preside_work,
                               u.office_phone1,
                               ur.sort
                          FROM dams_user_branch_rel ur,
                               dams_branch          b,
                               dams_user            u
                         WHERE ur.stru_id = i_stru_id
                           AND ur.stru_id = b.stru_id
                           AND u.ssic_id = ur.ssic_id
                           AND v_tmp_count > 0
                        UNION ALL
                        SELECT u.name,
                               s.stru_sname,
                               s.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               u.if_preside_work,
                               u.office_phone1,
                               u.sort
                          FROM dams_branch s, dams_user u
                         WHERE u.stru_id = s.stru_id
                           AND EXISTS
                         (SELECT 1
                                  FROM dams_branch_relation b
                                 WHERE b.up_bnch_id = v_stru_id
                                   AND b.bnch_id = s.stru_id)
                           AND u.name LIKE '%' || i_qname || '%'
                        UNION ALL
                        SELECT u.name,
                               s1.stru_sname,
                               s1.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               s2.if_preside_work,
                               u.office_phone1,
                               s2.sort
                          FROM dams_branch          s1,
                               dams_user_branch_rel s2,
                               dams_user            u
                         WHERE s1.stru_id = s2.stru_id
                           AND u.ssic_id = s2.ssic_id
                           AND EXISTS
                         (SELECT 1
                                  FROM dams_branch_relation b
                                 WHERE b.up_bnch_id = v_stru_id
                                   AND b.bnch_id = s2.stru_id)
                           AND u.name LIKE '%' || i_qname || '%') a
                  LEFT JOIN dams_prm_user_nls n
                    ON n.code = a.duty_lv
                   AND n.dict_kind = 'DUTY_LV'
                   AND lower(n.locale) = lower(i_lang)
                  LEFT JOIN dams_prm_user_nls n1
                    ON n1.code = a.duty_lv
                   AND n1.dict_kind = 'DUTY_LV'
                   AND n1.locale = 'zh_CN')
         WHERE rn < 100;
    ELSE
      v_arrall := pckg_dams_util.func_str_to_array(i_stru_id, ';', '1'); 
      OPEN o_list FOR
        SELECT text, ssic_id
          FROM (SELECT a.name || '|' || a.stru_sname || '|' || a.stru_id || '|' ||
                       nvl(n.name, n1.name) || '|' || a.office_phone1 text,
                       a.ssic_id,
                       row_number() over(ORDER BY a.duty_sort DESC NULLS LAST, nvl(a.if_preside_work, '502002') ASC, a.sort DESC NULLS LAST, a.name ASC NULLS LAST) rn
                  FROM (SELECT u.name,
                               s.stru_sname,
                               s.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               u.if_preside_work,
                               u.office_phone1,
                               u.sort
                          FROM dams_branch s, dams_user u
                         WHERE u.stru_id = s.stru_id
                           AND EXISTS
                         (SELECT 1
                                  FROM dams_branch_relation b
                                 WHERE instr(i_stru_id, b.up_bnch_id) > 0
                                   AND b.bnch_id = s.stru_id)
                           AND u.name LIKE '%' || i_qname || '%'
                        UNION ALL
                        SELECT u.name,
                               s1.stru_sname,
                               s1.stru_id,
                               u.duty_lv,
                               u.ssic_id,
                               nvl(u.duty_sort, 0) duty_sort,
                               s2.if_preside_work,
                               u.office_phone1,
                               s2.sort
                          FROM dams_branch          s1,
                               dams_user_branch_rel s2,
                               dams_user            u
                         WHERE s1.stru_id = s2.stru_id
                           AND u.ssic_id = s2.ssic_id
                           AND EXISTS
                         (SELECT 1
                                  FROM dams_branch_relation b
                                 WHERE instr(i_stru_id, b.up_bnch_id) > 0
                                   AND b.bnch_id = s2.stru_id)
                           AND u.name LIKE '%' || i_qname || '%') a
                  LEFT JOIN dams_prm_user_nls n
                    ON n.code = a.duty_lv
                   AND n.dict_kind = 'DUTY_LV'
                   AND lower(n.locale) = lower(i_lang)
                  LEFT JOIN dams_prm_user_nls n1
                    ON n1.code = a.duty_lv
                   AND n1.dict_kind = 'DUTY_LV'
                   AND n1.locale = 'zh_CN')
         WHERE rn < 100;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      --需要主动关闭游标
      IF o_list%ISOPEN THEN
        CLOSE o_list;
      END IF;
      o_ret_code := '-1';
      RETURN;
  END proc_query_stru_user;
  
  PROCEDURE proc_get_stru_info(i_stru_name IN VARCHAR2, --输入部门信息
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_list      OUT SYS_REFCURSOR --返回用户信息
                               ) IS
    v_param     db_log.info%TYPE := i_stru_name;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_common.proc_get_stru_info';
    v_step      db_log.step_no%TYPE;
  BEGIN
    o_ret_code := 0;
    OPEN o_list FOR
      SELECT p.stru_sname text, p.stru_id VALUE
        FROM (SELECT s.stru_sname, s.stru_id, rownum rn
                FROM dams_branch s
               WHERE s.stru_fname LIKE '%' || i_stru_name || '%') p
       WHERE rn < 50;
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
      IF o_list%ISOPEN THEN
        CLOSE o_list;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_stru_info;
  
  PROCEDURE proc_reset_natural_holiday(i_bgn_date IN VARCHAR2, --开始日期
                                       i_end_date IN VARCHAR2, --结束日期
                                       o_ret_code OUT VARCHAR2 --返回值
                                       ) IS
    v_param     db_log.info%TYPE := i_bgn_date || '|' || i_end_date;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_common.proc_reset_natural_holiday';
    v_step      db_log.step_no%TYPE;
    v_bgn_date  DATE;
    v_end_date  DATE;
  BEGIN
    o_ret_code := 0;
    SELECT to_date(i_bgn_date, 'YYYY-MM-DD'), to_date(i_end_date, 'YYYY-MM-DD') INTO v_bgn_date, v_end_date FROM dual;
    dbms_output.put_line('v_bgn_date=' || v_bgn_date || ' v_end_date='||v_end_date);
	LOOP
      EXIT WHEN v_bgn_date >= v_end_date;
      INSERT INTO dams_prm_holiday_flag
        (natural_date, holiday_flag)
      VALUES
        (to_char(v_bgn_date, 'YYYYMMDD'),
         decode(to_char(v_bgn_date, 'D'), '1', '1', '7', '1', '0'));
      v_bgn_date := v_bgn_date + 1;
    END LOOP;
    COMMIT;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      ROLLBACK;
      o_ret_code := -1;
      RETURN;
  END proc_reset_natural_holiday;
  
  PROCEDURE proc_set_natural_holiday(i_bgn_date   IN VARCHAR2, --开始日期
                                     i_end_date   IN VARCHAR2, --结束日期
                                     i_is_holiday IN VARCHAR2, --是否节假日 1-是，2-否
                                     o_ret_code   OUT VARCHAR2 --返回值
                                     ) IS
    v_param     db_log.info%TYPE := i_bgn_date || '|' || i_end_date || '|' ||
                                    i_is_holiday;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_common.proc_set_natural_holiday';
    v_step      db_log.step_no%TYPE;
    v_bgn_date  VARCHAR2(8);
    v_end_date  VARCHAR2(8);
  BEGIN
    o_ret_code := 0;
    SELECT REPLACE(i_bgn_date, '-', ''), REPLACE(i_end_date, '-', '')
      INTO v_bgn_date, v_end_date
      FROM dual;
    UPDATE dams_prm_holiday_flag t
       SET t.holiday_flag = i_is_holiday
     WHERE t.natural_date >= v_bgn_date
       AND t.natural_date <= v_end_date;
    COMMIT;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      ROLLBACK;
      o_ret_code := -1;
      RETURN;
  END proc_set_natural_holiday;
  
  PROCEDURE proc_cacu_day_span(i_include_holiday IN VARCHAR2, --是否包含节假日 1-包含 0-不包含
                               i_bgn_date        IN VARCHAR2, --开始日期
                               i_bgn_time_type   IN VARCHAR2, --开始时间类型   1-上午 2-下午
                               i_end_date        IN VARCHAR2, --结束日期
                               i_end_time_type   IN VARCHAR2, --结束时间类型   1-上午 2-下午
                               o_ret_code        OUT VARCHAR2, --返回值
                               o_day_num         OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_include_holiday || '|' || i_bgn_date || '|' ||
                                    i_bgn_time_type || '|' || i_end_date || '|' ||
                                    i_end_time_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_common.proc_cacu_day_span';
    v_step      db_log.step_no%TYPE;
    v_bgn_date  VARCHAR2(8);
    v_end_date  VARCHAR2(8);
    v_morning   NUMBER;
    v_afternoon NUMBER;
    v_day_num   NUMBER;
  BEGIN
    o_ret_code := 0;
    SELECT REPLACE(i_bgn_date, '-', ''), REPLACE(i_end_date, '-', '')
      INTO v_bgn_date, v_end_date
      FROM dual;
    SELECT COUNT('A')
      INTO v_day_num
      FROM dams_prm_holiday_flag t
     WHERE t.natural_date >= v_bgn_date
       AND t.natural_date <= v_end_date
       AND (i_include_holiday = '1' OR
           (t.holiday_flag <> '1' AND i_include_holiday = '0'));
    IF i_bgn_time_type = '1' THEN
      v_morning := 0;
    ELSE
      v_morning := 0.5;
    END IF;
    IF i_end_time_type = '1' THEN
      v_afternoon := 0.5;
    ELSE
      v_afternoon := 1;
    END IF;
    v_day_num := v_day_num + v_afternoon - v_morning - 1;
    IF v_day_num = 0.5 THEN
      o_day_num := '0.5';
    ELSE
      o_day_num := v_day_num;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      o_day_num  := 0;
      RETURN;
  END proc_cacu_day_span;
  
  PROCEDURE proc_branch_tree(i_id          IN VARCHAR2, --机构编码
                             i_rawid       IN VARCHAR2, --原始节点编码，判断根节点
                             i_system      IN VARCHAR2, --系统编码
                             o_flag        OUT VARCHAR2, --存储过程返回标志
                             o_branch_list OUT SYS_REFCURSOR --角色列表
                             ) IS
    v_param     db_log.info%TYPE := i_id || '|' || i_rawid || '|' || i_system;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_branch_tree';
    v_mv_name   VARCHAR2(30) := '';
    v_sql       VARCHAR2(1000) := '';
    v_id        VARCHAR2(20) := '';
    v_count     NUMBER(6);
    v_strlist   dams_arrytype;
    struarray arrytype;
    TYPE array_type IS VARRAY(50) OF NUMBER;
    codevar array_type;
    v_tempstru  VARCHAR2(4000);
    v_inusestru VARCHAR2(4000) := '';
    v_flag      NUMBER := 1;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    IF i_id = '0' OR i_id IS NULL THEN
      IF i_rawid IS NULL THEN
        --取根节点（总行）
        OPEN o_branch_list FOR
          SELECT b.stru_id, b.stru_sname, '0' isleaf, b.sup_stru sup_stru
            FROM dams_branch b
           WHERE b.sup_stru IS NULL
                --modify by haozj refer to 网讯通讯录
             AND nvl(b.stru_lv, '0') NOT IN
                 ('73', '74', '75', '76', '77', '79', '80')
             AND b.stru_sign <> '6'
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      ELSE
        --i_rawid 0460000005,0010100023,0010100500
        --根据层级,去掉子机构
        --i_rawid --->0010100500 
        v_strlist := pckg_dams_util.func_str_to_array(i_rawid, ',');
        FOR v_i IN 1 .. v_strlist.count
        LOOP
          INSERT INTO si_notes_tmp (id) VALUES (v_strlist(v_i));
        END LOOP;
        codevar := array_type();
        SELECT id BULK COLLECT INTO struarray FROM si_notes_tmp; 
        FOR i IN 1 .. struarray.count
        LOOP
          FOR j IN 1 .. struarray.count
          LOOP
            IF j != i THEN
              SELECT listagg(t.stru_id) within GROUP(ORDER BY 1) 
                INTO v_tempstru
                FROM dams_branch t
               WHERE t.stru_id != struarray(i)
               START WITH t.stru_id = struarray(i)
              CONNECT BY PRIOR t.sup_stru = t.stru_id;
              IF instr(v_tempstru, struarray(j)) > 0 THEN
                codevar.extend();
                codevar(v_flag) := i;
                v_flag := v_flag + 1;
                EXIT;
              END IF;
            END IF;
          END LOOP;
        END LOOP;   
        IF codevar IS NOT NULL THEN
          FOR k IN 1 .. struarray.count
          LOOP
            FOR l IN 1 .. codevar.count
            LOOP
              IF codevar(l) IS NOT NULL AND k = codevar(l) THEN
                struarray(k) := '';
              END IF;
            END LOOP;
          END LOOP;
        END IF;
        FOR m IN 1 .. struarray.count
        LOOP
          v_inusestru := v_inusestru || ',' || struarray(m);
        END LOOP;
        v_inusestru := v_inusestru || ',';
        --取该节点为根节点
        OPEN o_branch_list FOR
          SELECT b.stru_id, b.stru_sname, '0' isleaf, b.sup_stru sup_stru
            FROM dams_branch b
           WHERE --b.stru_id IN (SELECT id FROM si_notes_tmp)
           instr(v_inusestru, ',' || b.stru_id || ',') > 0
          --modify by haozj refer to 网讯通讯录
           AND nvl(b.stru_lv, '0') NOT IN
           ('73', '74', '75', '76', '77', '79', '80')
           AND b.stru_sign <> '6'
           AND b.stru_sname NOT LIKE '%删除%'
           AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      END IF;
    ELSE
      IF (i_system = 'ST2') THEN
        --保密机构树，只出分支机构
        --指定i_id，点击展开时异步获取子树
        --兼容传本部
        v_id := i_id;
        SELECT COUNT(1)
          INTO v_count
          FROM dams_branch t
         WHERE t.stru_id = i_id
           AND t.stru_sign = '2';
        IF v_count > 0 THEN
          SELECT t.sup_stru
            INTO v_id
            FROM dams_branch t
           WHERE t.stru_id = i_id;
        END IF;
        OPEN o_branch_list FOR
          SELECT stru_id,
                 stru_sname,
                 CASE
                   WHEN (SELECT 1
                           FROM dual
                          WHERE EXISTS
                          (SELECT 1
                                   FROM dams_branch
                                  WHERE sup_stru = b.stru_id
                                    AND nvl(stru_lv, '0') NOT IN
                                        ('73', '74', '75', '76', '77', '79', '80')
                                    AND (stru_sign = '1' AND
                                        stru_grade IN ('1', '2', '3', '4') OR
                                        stru_sign IN ('8', '18') AND
                                        man_grade = '1' OR
                                        stru_sign = '4' AND
                                        man_grade IN ('1', '2'))
                                    AND stru_sname NOT LIKE '%删除%'
                                    AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                    '0'
                   ELSE
                    '1'
                 END isleaf, --是否有叶节点
                 sup_stru
            FROM dams_branch b
           WHERE b.sup_stru = v_id
             AND (b.stru_sign = '1' AND b.stru_grade IN ('1', '2', '3', '4') OR
                 b.stru_sign IN ('8', '18') AND b.man_grade = '1' OR
                 b.stru_sign = '4' AND b.man_grade IN ('1', '2'))
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      ELSIF (i_system = 'ST3') THEN
        SELECT substr(i_id, instr(i_id, '$', -1, 1) + 1) INTO v_id FROM dual;
        OPEN o_branch_list FOR
          SELECT stru_id,
                 stru_sname,
                 CASE
                   WHEN b.stru_id = '0010100500' THEN
                    '0'
                   ELSE
                    '1'
                 END isleaf,
                 sup_stru
            FROM dams_branch b
           WHERE b.sup_stru = v_id
                --modify by haozj refer to 网讯通讯录
             AND nvl(b.stru_lv, '0') NOT IN
                 ('73', '74', '75', '76', '77', '79', '80')
             AND b.stru_sign <> '6'
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      ELSIF (i_system = 'ST4') THEN
        --指定i_id，点击展开时异步获取子树
        --兼容传本部
        v_id := i_id;
        SELECT COUNT(1)
          INTO v_count
          FROM dams_branch t
         WHERE t.stru_id = i_id
           AND t.stru_sign = '2';
        IF v_count > 0 THEN
          OPEN o_branch_list FOR
            SELECT stru_id,
                   stru_sname,
                   '1' isleaf, --是否有叶节点
                   sup_stru
              FROM dams_branch b
             WHERE b.sup_stru = v_id
               AND b.stru_sname NOT LIKE '%删除%'
               AND b.stru_state IN ('1', '2')
             ORDER BY b.stru_sort DESC NULLS LAST,
                      b.sort DESC NULLS LAST,
                      b.stru_sname;
        ELSE
          OPEN o_branch_list FOR
            SELECT stru_id,
                   stru_sname,
                   CASE
                     WHEN ((SELECT 1
                              FROM dual
                             WHERE EXISTS
                             (SELECT 1
                                      FROM dams_branch
                                     WHERE sup_stru = b.stru_id
                                       AND nvl(stru_lv, '0') NOT IN
                                           ('73', '74', '75', '76', '77', '79',
                                            '80')
                                       AND (stru_sign = '1' AND
                                           stru_grade IN ('1', '2', '3', '4') OR
                                           stru_sign IN ('8', '18') AND
                                           man_grade = '1' OR
                                           stru_sign = '4' AND
                                           man_grade IN ('1', '2'))
                                       AND stru_sname NOT LIKE '%删除%'
                                       AND stru_state IN ('1', '2'))) IS NOT NULL OR
                          b.stru_sign = '2') THEN
                      '0'
                     ELSE
                      '1'
                   END isleaf, --是否有叶节点
                   sup_stru
              FROM dams_branch b
             WHERE b.sup_stru = v_id
               AND ((b.stru_sign = '1' AND b.stru_grade IN ('1', '2', '3', '4') OR
                   b.stru_sign IN ('8', '18') AND b.man_grade = '1' OR
                   b.stru_sign = '4' AND b.man_grade IN ('1', '2')) OR
                   (b.stru_sign = '2' AND v_id = i_rawid))
               AND b.stru_sname NOT LIKE '%删除%'
               AND b.stru_state IN ('1', '2')
             ORDER BY b.stru_sort DESC NULLS LAST,
                      b.sort DESC NULLS LAST,
                      b.stru_sname;
        END IF;
      ELSE
        --取传入的id为根节点
        SELECT substr(i_id, instr(i_id, '$', -1, 1) + 1) INTO v_id FROM dual;
        OPEN o_branch_list FOR
          SELECT stru_id,
                 stru_sname,
                 CASE
                   WHEN (SELECT 1
                           FROM dual
                          WHERE EXISTS (SELECT 1
                                   FROM dams_branch
                                  WHERE sup_stru = b.stru_id)) IS NOT NULL THEN
                    '0'
                   ELSE
                    '1'
                 END isleaf,
                 sup_stru
            FROM dams_branch b
           WHERE b.sup_stru = v_id
                --modify by haozj refer to 网讯通讯录
             AND nvl(b.stru_lv, '0') NOT IN
                 ('73', '74', '75', '76', '77', '79', '80')
             AND b.stru_sign <> '6'
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      IF o_branch_list%ISOPEN THEN
        CLOSE o_branch_list;
      END IF;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_branch_tree;
  
  PROCEDURE proc_get_stru_name(i_stru_id   IN VARCHAR2, --机构编码
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_stru_name OUT VARCHAR2 --机构路径
                               ) IS
  BEGIN
    o_ret_code := 0;
    SELECT func_get_stru_name(i_stru_id) INTO o_stru_name FROM dual;
  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := -1;
  END proc_get_stru_name;

  FUNCTION func_get_stru_name(i_stru_id IN VARCHAR2 --机构编码
                              ) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
    SELECT t.stru_sname
      INTO v_rlt
      FROM dams_branch t
     WHERE t.stru_id = i_stru_id;
  
    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN i_stru_id;
  END func_get_stru_name;
  
  PROCEDURE proc_get_stru_name_bat(i_stru_ids IN VARCHAR2, --机构编码
                                   o_ret_code OUT VARCHAR2, --返回信息
                                   o_list     OUT SYS_REFCURSOR --机构路径
                                   ) IS
  BEGIN
    o_ret_code := 0;
    OPEN o_list FOR
      SELECT b.stru_id, b.stru_sname
        FROM dams_branch b,
             (SELECT column_value stru_id, rownum rn
                FROM TABLE(func_split2(i_stru_ids))) t
       WHERE b.stru_id = t.stru_id
       ORDER BY rn;
  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := -1;
  END proc_get_stru_name_bat;
  
  PROCEDURE proc_get_stru_path(i_stru_id   IN VARCHAR2, --机构编码
                               i_sep       IN VARCHAR2, --分隔符
                               i_flag      IN VARCHAR2, --0,向上取至一级企业
                               o_ret_code  OUT VARCHAR2, --返回信息
                               o_stru_name OUT VARCHAR2 --机构路径
                               ) IS
  BEGIN
    o_ret_code := 0;
    SELECT REPLACE(listagg(tt.stru_sname) within GROUP(ORDER BY tt.stru_sname NULLS LAST), ',', i_sep)
      INTO o_stru_name
      FROM (SELECT t.stru_sname
              FROM dams_branch t
             START WITH t.stru_id = i_stru_id
            CONNECT BY t.stru_id = PRIOR t.sup_stru
                   AND PRIOR t.sup_stru <> '0010100000'
             ORDER BY LEVEL DESC) tt;
  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := -1;
  END proc_get_stru_path;
  
  FUNCTION func_get_stru_path(i_stru_id IN VARCHAR2, --机构编码
                              i_sep     IN VARCHAR2 := '/', --分隔符
                              i_flag    IN VARCHAR2 := 0 --0,向上取至一级企业
                              ) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
  
    SELECT REPLACE(listagg(tt.stru_sname) within GROUP(ORDER BY tt.stru_sname NULLS LAST), ',', i_sep)
      INTO v_rlt
      FROM (SELECT t.stru_sname
              FROM dams_branch t
             START WITH t.stru_id = i_stru_id
            CONNECT BY t.stru_id = PRIOR t.sup_stru
                   AND PRIOR t.sup_stru <> '0010100000'
             ORDER BY LEVEL DESC) tt;
    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_stru_path;
  
  PROCEDURE proc_process_close(o_retcode OUT VARCHAR2,
                               o_msg     OUT VARCHAR2) IS
  
    v_param     db_log.info%TYPE;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_process_close';
    v_step      db_log.step_no%TYPE;
    v_add_time  DATE;
    v_app_id    VARCHAR2(30);
    v_list      SYS_REFCURSOR;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := 0;
    OPEN v_list FOR
    --安全例外申请
      SELECT app_id
        FROM (SELECT t.app_id,
                     CASE
                       WHEN add_months(t.deny_time, 1) - SYSDATE <= 0 THEN
                        1
                       ELSE
                        0
                     END add_date
                FROM sa_sec_excep_app t)
       WHERE add_date = 1
      UNION ALL  
      --信息安全审查申请
      SELECT app_id
        FROM (SELECT t.app_id,
                     CASE
                       WHEN add_months(t.deny_time, 1) - SYSDATE <= 0 THEN
                        1
                       ELSE
                        0
                     END add_date
                FROM sa_sec_exam_app t)
       WHERE add_date = 1
      UNION ALL
      --外发邮件申请
      SELECT app_id
        FROM (SELECT t.app_id,
                     CASE
                       WHEN add_months(t.deny_time, 1) - SYSDATE <= 0 THEN
                        1
                       ELSE
                        0
                     END add_date
                FROM sa_sec_exam_app t)
       WHERE add_date = 1
      UNION ALL
      --网吧机特殊软件申请
      SELECT app_id
        FROM (SELECT t.app_id,
                     CASE
                       WHEN                      
                        add_months(t.deny_time, 1) - SYSDATE <= 0 THEN
                        1
                       ELSE
                        0
                     END add_date
                FROM sa_pub_soft_app t)
       WHERE add_date = 1;
    LOOP
      FETCH v_list
        INTO v_app_id;
      EXIT WHEN v_list%NOTFOUND;
      --废止申请
      --pckg_dams_task.proc_del_task('', v_app_id, '0', o_retcode, o_msg);
    END LOOP;
    CLOSE v_list;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line(SQLERRM);
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
  END proc_process_close;
  
  PROCEDURE proc_dams_logger(i_logobj     IN VARCHAR2,
                             i_logtype    IN VARCHAR2,
                             i_logdate    IN VARCHAR2,
                             i_loghandler IN VARCHAR2,
                             i_loginfo    IN VARCHAR2,
                             i_logdetail  IN CLOB := '') IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_key  VARCHAR2(20);
    v_info VARCHAR2(4000);
  BEGIN
    SELECT lpad(dams_syslog_seq.nextval, 20, '0') INTO v_key FROM dual;
    v_info := substrb(i_loginfo, 1, 4000);
    INSERT INTO dams_operation_loginfo
      (keyid, logobj, logtype, logdate, loghandler, loginfo, logdetail)
    VALUES
      (v_key,
       i_logobj,
       i_logtype,
       i_logdate,
       i_loghandler,
       i_loginfo,
       i_logdetail);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END proc_dams_logger;
  
  FUNCTION func_get_deptlevel1(i_stru_id IN VARCHAR2, --机构编码
                               i_type    IN VARCHAR2 := '0' --0返回机构编码 1返回机构名称
                               ) RETURN VARCHAR2 IS
    v_stru          dams_branch.stru_id%TYPE := i_stru_id;
    v_result        dams_branch.stru_id%TYPE := '';
    v_stru_sign     dams_branch.stru_sign%TYPE := '';
    v_stru_name     dams_branch.stru_sname%TYPE := '';
    v_sup_stru_name dams_branch.stru_sname%TYPE := '';
    v_count         NUMBER(2) := 0;
  BEGIN
    LOOP
      v_count := v_count + 1;
      SELECT s.stru_sign, s.stru_id, t.stru_id, t.stru_sname, s.stru_sname
        INTO v_stru_sign, v_stru, v_result, v_stru_name, v_sup_stru_name
        FROM dams_branch t, dams_branch s
       WHERE t.sup_stru = s.stru_id
         AND t.stru_state IN ('1', '2')
         AND t.stru_id = v_stru;  
      IF v_stru_sign = '2' THEN
        --境内及直属机构，有本部的情况
        EXIT;
      ELSIF instr(v_sup_stru_name, '本部') > 0 THEN
        --境外分行本部
        EXIT;
      ELSIF v_stru = '0011000000' THEN
        --工银亚洲
        EXIT;
      ELSIF v_count > 7 THEN
        --防止死循环
        v_result    := '';
        v_stru_name := '';
        EXIT;
      END IF;
    END LOOP;
    IF i_type = '0' THEN
      RETURN v_result;
    ELSE
      RETURN v_stru_name;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_deptlevel1;
  
  PROCEDURE proc_get_deptlevel1(i_stru_id  IN VARCHAR2, --输入机构
                                o_ret_code OUT VARCHAR2,
                                o_msg      OUT VARCHAR2,
                                o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_deptlevel1';
    v_step      db_log.step_no%TYPE;
    v_stru          dams_branch.stru_id%TYPE := i_stru_id;
    v_lv1_stru_id   dams_branch.stru_id%TYPE := '';
    v_stru_sign     dams_branch.stru_sign%TYPE := '';
    v_lv1_stru_name dams_branch.stru_sname%TYPE := '';
    v_count         NUMBER(2) := 0;
    v_office_stru_id   dams_branch.stru_id%TYPE := ''; --处室
    v_office_stru_name dams_branch.stru_sname%TYPE := ''; --处室
  BEGIN
    o_ret_code := 0;
    IF i_stru_id IS NULL THEN
      OPEN o_list FOR
        SELECT '' stru_id,
               '' stru_name,
               '' lv1_stru_id,
               '' lv1_stru_name,
               '' office_stru_id,
               '' office_stru_name
          FROM dual;
    ELSE
      LOOP
        v_count := v_count + 1;   
        SELECT s.stru_sign, s.stru_id, t.stru_id, t.stru_sname
          INTO v_stru_sign, v_stru, v_lv1_stru_id, v_lv1_stru_name
          FROM dams_branch t, dams_branch s
         WHERE t.sup_stru = s.stru_id
           AND t.stru_state IN ('1', '2')
           AND t.stru_id = v_stru;   
        IF v_stru_sign = '2' THEN
          EXIT;
        ELSIF v_count > 7 THEN
          --防止死循环
          v_lv1_stru_id   := '';
          v_lv1_stru_name := '';
          EXIT;
        END IF; 
      END LOOP;
      SELECT b.stru_id, b.stru_sname
        INTO v_office_stru_id, v_office_stru_name
        FROM dams_branch b
       WHERE b.stru_id = i_stru_id; 
      IF i_stru_id = v_lv1_stru_id THEN
        v_office_stru_id   := NULL;
        v_office_stru_name := NULL;
      END IF;
      OPEN o_list FOR
        SELECT b.stru_id,
               b.stru_sname       stru_name,
               v_lv1_stru_id      lv1_stru_id,
               v_lv1_stru_name    lv1_stru_name,
               v_office_stru_id   office_stru_id,
               v_office_stru_name office_stru_name
          FROM dams_branch b
         WHERE b.stru_id = i_stru_id;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
      IF o_list%ISOPEN THEN
        CLOSE o_list;
      END IF;
      OPEN o_list FOR
        SELECT '' stru_id,
               '' stru_name,
               '' lv1_stru_id,
               '' lv1_stru_name,
               '' office_stru_id,
               '' office_stru_name
          FROM dual;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_deptlevel1;
  
  --PROCEDURE proc_get_st_pdkey(i_stru_id  IN VARCHAR2, --机构
  --                            i_sec_lv   IN VARCHAR2, --密级
  --                            i_dsm_type IN VARCHAR2, --涉密文档提供类型：1复制 2对外提供
  --                            o_ret_code OUT VARCHAR2,
  --                            o_msg      OUT VARCHAR2,
  --                            o_pdkey    OUT VARCHAR2,
  --                            o_biztype  OUT VARCHAR2) IS
  --  v_param     db_log.info%TYPE := i_stru_id || '|' || i_sec_lv || '|' ||
  --                                  i_dsm_type;
  --  v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_st_pdkey';
  --  v_step      db_log.step_no%TYPE;
  --  v_stru_level VARCHAR2(10);
  --  v_pdkey      VARCHAR2(255);
  --  v_biztype    VARCHAR2(20);
  --  v_count      NUMBER(5);
  --BEGIN
  --  "$log"(310);pack_log.info(v_proc_name, pack_log.start_step,
  --                pack_log.start_msg || v_param);
  --  "$log"(311);o_ret_code := 0;
  --  "$log"(312);IF i_stru_id IS NULL THEN
  --    "$log"(313);o_ret_code := -2;
  --    "$log"(314);o_msg      := '请选择您的申请部门！';
  --    "$log"(315);o_pdkey    := '';
  --    "$log"(316);pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  --    "$log"(317);RETURN;
  --  END IF;
  --  "$log"(318);SELECT MAX(t.stru_level)
  --    INTO v_stru_level
  --    FROM dams_branch_catalog t
  --   WHERE t.stru_id = i_stru_id;
  --  --8  境外机构
  --  --18 境内控股机构
  --  "$log"(319);SELECT COUNT(1)
  --    INTO v_count
  --    FROM dams_branch_relation t
  --   WHERE t.bnch_id = i_stru_id
  --     AND t.bnch_sign IN ('8', '18');
  --  "$log"(320);IF v_count > 0 THEN
  --    "$log"(321);v_stru_level := '9';
  --  END IF;
  --  "$log"(322);IF v_stru_level IS NULL THEN
  --    "$log"(323);o_ret_code := -2;
  --    "$log"(324);o_msg      := '获取流程PDKEY失败';
  --    "$log"(325);o_pdkey    := '';
  --  ELSE
  --    "$log"(326);IF i_dsm_type = 'dsmcopy' THEN
  --      "$log"(327);v_pdkey := 'stdsmcopy';
  --    ELSIF i_dsm_type = 'dsmprovide' THEN
  --      "$log"(328);v_pdkey := 'stdsmprovide';
  --    END IF;
  --    --1总行 2一级行 3,4二级行 9境外机构
  --    "$log"(329);IF v_stru_level = '1' THEN
  --      "$log"(330);v_pdkey := v_pdkey || '_' || 'head';
  --    ELSIF v_stru_level = '2' THEN
  --      "$log"(331);v_pdkey := v_pdkey || '_' || 'bnch';
  --    ELSIF v_stru_level = '3' OR v_stru_level = '4' THEN
  --      "$log"(332);v_pdkey := v_pdkey || '_' || 'sub_bnch';
  --    ELSIF v_stru_level = '9' THEN
  --      "$log"(333);v_pdkey := v_pdkey || '_' || 'os_bnch';
  --    END IF;
  --    "$log"(334);SELECT COUNT(1)
  --      INTO v_count
  --      FROM dams_wf_process_conf t
  --     WHERE t.process_key = v_pdkey
  --       AND t.sys_code = 'QXSQ'; 
  --    "$log"(335);SELECT CASE v_pdkey
  --             WHEN 'stdsmcopy_head' THEN
  --              'ST03'
  --             WHEN 'stdsmcopy_bnch' THEN
  --              'ST04'
  --             WHEN 'stdsmcopy_sub_bnch' THEN
  --              'ST05'
  --             WHEN 'stdsmcopy_os_bnch' THEN
  --              'ST06'
  --             WHEN 'stdsmprovide_head' THEN
  --              'ST07'
  --             WHEN 'stdsmprovide_bnch' THEN
  --              'ST08'
  --             WHEN 'stdsmprovide_sub_bnch' THEN
  --              'ST09'
  --             WHEN 'stdsmprovide_os_bnch' THEN
  --              'ST10'
  --           END biztype
  --      INTO v_biztype
  --      FROM dual; 
  --    "$log"(336);IF v_count > 0 THEN
  --      "$log"(337);o_pdkey   := v_pdkey;
  --      "$log"(338);o_biztype := v_biztype;
  --    END IF;
  --  END IF;
  --  "$log"(339);pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    --dbms_output.put_line(SQLERRM);
  --    "$log"(340);pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --    "$log"(341);o_msg      := '获取流程PDKEY失败';
  --    "$log"(342);o_pdkey    := '';
  --    "$log"(343);o_ret_code := -1;
  --END proc_get_st_pdkey;
  
  FUNCTION func_get_lv1_branch(i_stru_id IN VARCHAR2 --机构编码
                               ) RETURN VARCHAR2 IS
    v_stru_id VARCHAR2(10) := '';
  BEGIN
    SELECT stru_id
      INTO v_stru_id
      FROM (SELECT t.up_bnch_id stru_id
              FROM dams_branch_relation t
             WHERE t.bnch_id = i_stru_id
               AND (t.up_bnch_sign = '1' AND
                   t.up_bnch_grade IN ('1', '2', '3', '4') OR
                   t.up_bnch_sign = '4' OR
                   t.up_bnch_sign IN ('8', '18') AND t.up_bnch_man_grade = '1')
             ORDER BY t.up_level)
     WHERE rownum = 1;
    RETURN v_stru_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_lv1_branch;

  FUNCTION func_get_lv2_branch(i_stru_id IN VARCHAR2 --机构编码
                               ) RETURN VARCHAR2 IS
    v_stru_id   VARCHAR2(10) := '';
    v_stru_sign VARCHAR2(10) := '';
    v_tmp_count NUMBER(5);
  BEGIN
    SELECT b.stru_sign
      INTO v_stru_sign
      FROM dams_branch b
     WHERE b.stru_id = i_stru_id;
    IF v_stru_sign NOT IN ('1', '4') THEN
      v_stru_id := i_stru_id;
    END IF;
    SELECT COUNT(1)
      INTO v_tmp_count
      FROM dams_branch b
     WHERE b.sup_stru = i_stru_id
       AND b.stru_sign = '2'
       AND b.stru_state IN ('1', '2');
    IF v_tmp_count > 0 THEN
      SELECT b.stru_id
        INTO v_stru_id
        FROM dams_branch b
       WHERE b.sup_stru = i_stru_id
         AND b.stru_sign = '2'
         AND b.stru_state IN ('1', '2')
         AND rownum = 1;
    ELSE
      v_stru_id := i_stru_id;
    END IF;
    RETURN v_stru_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_lv2_branch;
  
  PROCEDURE proc_get_lv1_branch(i_stru_id    IN VARCHAR2, --机构编码
                                o_ret_code   OUT VARCHAR2, --返回信息
                                o_lv1_branch OUT VARCHAR2 --返回用户信息
                                ) IS
    v_param     db_log.info%TYPE := i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_lv1_branch';
    v_array     dams_arrytype := pckg_dams_util.func_str_to_array(i_stru_id, ';',
                                                                  '1');
  BEGIN
    o_ret_code := 0;
    IF (instr(i_stru_id, ';') > 0) THEN
      --SELECT dams_conn(a1.up_bnch_id)
      SELECT a1.up_bnch_id
        INTO o_lv1_branch
        FROM (SELECT br.up_bnch_id,
                     br.up_level,
                     MIN(br.up_level) over(PARTITION BY a.v1) min_level
                FROM (SELECT column_value v1 FROM TABLE(v_array)) a,
                     dams_branch_relation br
               WHERE a.v1 = br.bnch_id
                 AND (br.up_bnch_sign = '1' AND
                     br.up_bnch_grade IN ('1', '2', '3', '4') OR
                     br.up_bnch_sign = '4' OR
                     br.up_bnch_sign IN ('8', '18') AND
                     (SELECT b.man_grade
                         FROM dams_branch b
                        WHERE b.stru_id = br.up_bnch_id) = '1')) a1
       WHERE a1.up_level = a1.min_level;
    ELSE
      SELECT stru_id
        INTO o_lv1_branch
        FROM (SELECT t.up_bnch_id stru_id
                FROM dams_branch_relation t
               WHERE t.bnch_id = i_stru_id
                 AND (t.up_bnch_sign = '1' AND
                     t.up_bnch_grade IN ('1', '2', '3', '4') OR
                     t.up_bnch_sign = '4' OR
                     t.up_bnch_sign IN ('8', '18') AND
                     (SELECT b.man_grade
                         FROM dams_branch b
                        WHERE b.stru_id = t.up_bnch_id) = '1')
               ORDER BY t.up_level)
       WHERE rownum = 1;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_lv1_branch;

  PROCEDURE proc_get_lv2_branch(i_stru_id    IN VARCHAR2, --机构编码
                                o_ret_code   OUT VARCHAR2, --返回信息
                                o_lv2_branch OUT VARCHAR2 --返回用户信息
                                ) IS
    v_param     db_log.info%TYPE := i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_lv2_branch';
    v_stru_sign VARCHAR2(10) := '';
    v_tmp_count NUMBER(5);
  BEGIN
    o_ret_code := 0;
    SELECT b.stru_sign
      INTO v_stru_sign
      FROM dams_branch b
     WHERE b.stru_id = i_stru_id;
    IF v_stru_sign NOT IN ('1', '4') THEN
      o_lv2_branch := i_stru_id;
    END IF;
    SELECT COUNT(1)
      INTO v_tmp_count
      FROM dams_branch b
     WHERE b.sup_stru = i_stru_id
       AND b.stru_sign = '2'
       AND b.stru_state IN ('1', '2');
    IF v_tmp_count > 0 THEN
      SELECT b.stru_id
        INTO o_lv2_branch
        FROM dams_branch b
       WHERE b.sup_stru = i_stru_id
         AND b.stru_sign = '2'
         AND b.stru_state IN ('1', '2')
         AND rownum = 1;
    ELSE
      o_lv2_branch := i_stru_id;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_lv2_branch;
  
  PROCEDURE proc_user_choice_list(i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  i_branch_id IN VARCHAR2, --机构编码
                                  i_name      IN VARCHAR2, --
                                  i_sys_code  IN VARCHAR2,
                                  o_ret_code  OUT VARCHAR2, --存储过程返回标志
                                  o_msg       OUT VARCHAR2,
                                  o_total_num OUT VARCHAR2,
                                  o_user_list OUT SYS_REFCURSOR) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_user_choice_list';
    v_param     db_log.info%TYPE := i_beg_num || '|' || i_fetch_num || '|' ||
                                    i_branch_id || '|' || i_name;
    v_count1 NUMBER(5);
    v_has_hq NUMBER(5); 
    v_branch_id dams_branch.stru_id%TYPE;
  BEGIN
    o_ret_code := 0;
    CASE
      WHEN i_sys_code = 'ST' OR i_sys_code IS NULL THEN
        --是否有机构本部
        SELECT COUNT(1)
          INTO v_has_hq
          FROM dams_branch t
         WHERE t.stru_sign = '2'
           AND t.sup_stru = i_branch_id;
        IF v_has_hq = 1 THEN
          --有本部情况
          SELECT t.stru_id
            INTO v_branch_id
            FROM dams_branch t
           WHERE t.stru_sign = '2'
             AND t.sup_stru = i_branch_id;
        ELSE
          --无本部
          v_branch_id := i_branch_id;
        END IF;
        IF (i_name IS NOT NULL) THEN
          --有搜索条件i_name
          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT 1
                    FROM (SELECT u.ssic_id,
                                 u.person_id,
                                 u.name,
                                 u.stru_id,
                                 u.duty_lv,
                                 u.work_state work_state_code
                            FROM dams_user u
                          UNION ALL
                          SELECT dubr.ssic_id,
                                 du.person_id,
                                 du.name,
                                 dubr.stru_id,
                                 du.duty_lv,
                                 du.work_state work_state_code
                            FROM dams_user_branch_rel dubr, dams_user du
                           WHERE du.ssic_id = dubr.ssic_id) t1,
                         (SELECT b.stru_id, b.stru_sname
                            FROM dams_branch b, dams_branch_relation br
                           WHERE b.stru_id = br.bnch_id
                             AND (br.up_bnch_id = v_branch_id OR
                                 (br.bnch_id = i_branch_id AND
                                 br.up_bnch_id = i_branch_id))) t2
                   WHERE t1.stru_id = t2.stru_id
                     AND (t1.name LIKE '%' || i_name || '%' OR
                         t2.stru_sname LIKE '%' || i_name || '%')) t5;      
          OPEN o_user_list FOR
            SELECT ssic_id,
                   person_id,
                   NAME,
                   stru_id,
                   stru_sname sname,
                   work_state_code,
                   duty_lv,
                   (SELECT MAX(t.dictvalue)
                      FROM dams_gen_dict t
                     WHERE t.dicttype = 'DUTY_LV'
                       AND t.dictlang = 'zh_CN'
                       AND t.dictcode = duty_lv
                       AND t.sys_code = 'DAMS') duty_lv_name,
                   num,
                   ssic_id || '_' || stru_id id
              FROM (SELECT t1.ssic_id,
                           t1.person_id,
                           t1.name,
                           t1.stru_id,
                           t1.duty_lv,
                           t1.work_state_code,
                           t2.stru_sname,
                           t1.duty_sort,
                           t1.sort,
                           t1.if_preside_work,
                           row_number() over(ORDER BY t1.duty_sort DESC NULLS LAST, nvl(t1.if_preside_work, '502002'), t1.sort DESC NULLS LAST, t1.name ASC NULLS LAST) num
                      FROM (SELECT u.ssic_id,
                                   u.person_id,
                                   u.name,
                                   u.stru_id,
                                   u.duty_lv,
                                   u.work_state work_state_code,
                                   nvl(u.duty_sort, 0) duty_sort,
                                   u.sort,
                                   u.if_preside_work
                              FROM dams_user u
                            UNION ALL
                            SELECT dubr.ssic_id,
                                   du.person_id,
                                   du.name,
                                   dubr.stru_id,
                                   du.duty_lv,
                                   du.work_state work_state_code,
                                   nvl(du.duty_sort, 0) duty_sort,
                                   dubr.sort,
                                   dubr.if_preside_work
                              FROM dams_user_branch_rel dubr, dams_user du
                             WHERE du.ssic_id = dubr.ssic_id) t1,
                           (SELECT b.stru_id, b.stru_sname
                              FROM dams_branch b, dams_branch_relation br
                             WHERE b.stru_id = br.bnch_id
                               AND (br.up_bnch_id = v_branch_id OR
                                   (br.bnch_id = i_branch_id AND
                                   br.up_bnch_id = i_branch_id))) t2
                     WHERE t1.stru_id = t2.stru_id
                       AND (t1.name LIKE '%' || i_name || '%' OR
                           t2.stru_sname LIKE '%' || i_name || '%'))
             WHERE num < (to_number(i_beg_num) + to_number(i_fetch_num))
               AND num >= to_number(i_beg_num);
        ELSE
          --不包含i_name
          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT u.ssic_id, u.stru_id
                    FROM dams_user u
                  UNION ALL
                  SELECT dubr.ssic_id, dubr.stru_id
                    FROM dams_user_branch_rel dubr) t1,
                 dams_branch_relation t2
           WHERE t1.stru_id = t2.bnch_id
             AND (t2.up_bnch_id = v_branch_id OR
                 (t2.bnch_id = i_branch_id AND t2.up_bnch_id = i_branch_id));
          OPEN o_user_list FOR
            SELECT ssic_id,
                   person_id,
                   NAME,
                   stru_id,
                   sname,
                   work_state_code,
                   duty_lv,
                   (SELECT MAX(t.dictvalue)
                      FROM dams_gen_dict t
                     WHERE t.dicttype = 'DUTY_LV'
                       AND t.dictlang = 'zh_CN'
                       AND t.dictcode = duty_lv
                       AND t.sys_code = 'DAMS') duty_lv_name,
                   num,
                   ssic_id || '_' || stru_id id
              FROM (SELECT ssic_id,
                           person_id,
                           NAME,
                           t.stru_id,
                           stru_sname sname,
                           work_state_code,
                           duty_lv,
                           row_number() over(ORDER BY t1.duty_sort DESC NULLS LAST, nvl(t1.if_preside_work, '502002'), t1.sort DESC NULLS LAST, t1.name ASC NULLS LAST) num
                      FROM (SELECT u.ssic_id,
                                   u.person_id,
                                   u.name,
                                   u.stru_id,
                                   u.duty_lv,
                                   u.work_state work_state_code,
                                   nvl(u.duty_sort, 0) duty_sort,
                                   u.sort,
                                   u.if_preside_work
                              FROM dams_user u
                            UNION ALL
                            SELECT dubr.ssic_id,
                                   du.person_id,
                                   du.name,
                                   dubr.stru_id,
                                   du.duty_lv,
                                   du.work_state work_state_code,
                                   nvl(du.duty_sort, 0) duty_sort,
                                   dubr.sort,
                                   dubr.if_preside_work
                              FROM dams_user_branch_rel dubr, dams_user du
                             WHERE dubr.ssic_id = du.ssic_id) t1,
                           dams_branch_relation t2,
                           dams_branch t
                     WHERE t1.stru_id = t2.bnch_id
                       AND t2.bnch_id = t.stru_id
                       AND (t2.up_bnch_id = v_branch_id OR
                           (t2.bnch_id = i_branch_id AND
                           t2.up_bnch_id = i_branch_id)))
             WHERE num < (to_number(i_beg_num) + to_number(i_fetch_num))
               AND num >= to_number(i_beg_num);
        END IF;
        --保密委员会主任选人逻辑
      WHEN i_sys_code = 'STSECGP1' OR i_sys_code IS NULL THEN
        v_branch_id := pckg_dams_util.func_get_lv2_branch(i_branch_id);  
        SELECT COUNT(1)
          INTO o_total_num
          FROM (SELECT 1
                  FROM dams_user u, dams_branch_relation br, dams_branch b
                 WHERE EXISTS (SELECT 1
                          FROM dams_gen_dict gd
                         WHERE gd.dicttype = 'SECGP_DUTY'
                           AND gd.dictcode = u.duty_lv
                           AND gd.dictlang = 'zh_CN'
                           AND gd.sys_code = 'QXSQ')
                   AND u.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND u.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       u.name LIKE '%' || i_name || '%'))
                UNION ALL
                SELECT 1
                  FROM dams_user u, dams_branch b
                 WHERE EXISTS (SELECT 1
                          FROM dams_gen_dict gd
                         WHERE gd.dicttype = 'SECGP_DUTY'
                           AND gd.dictcode = u.duty_lv
                           AND gd.dictlang = 'zh_CN'
                           AND gd.sys_code = 'QXSQ')
                   AND u.stru_id = i_branch_id
                   AND u.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       u.name LIKE '%' || i_name || '%'))
                UNION ALL
                SELECT 1
                  FROM dams_user_branch_rel dubr,
                       dams_user            du,
                       dams_branch_relation br,
                       dams_branch          b
                 WHERE du.ssic_id = dubr.ssic_id
                   AND EXISTS (SELECT 1
                          FROM dams_gen_dict gd
                         WHERE gd.dicttype = 'SECGP_DUTY'
                           AND gd.dictcode = du.duty_lv
                           AND gd.sys_code = 'QXSQ')
                   AND dubr.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND dubr.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       du.name LIKE '%' || i_name || '%'))
                UNION ALL
                SELECT 1
                  FROM dams_user_branch_rel dubr, dams_user du, dams_branch b
                 WHERE du.ssic_id = dubr.ssic_id
                   AND EXISTS (SELECT 1
                          FROM dams_gen_dict gd
                         WHERE gd.dicttype = 'SECGP_DUTY'
                           AND gd.dictcode = du.duty_lv
                           AND gd.dictlang = 'zh_CN'
                           AND gd.sys_code = 'QXSQ')
                   AND du.stru_id = i_branch_id
                   AND dubr.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       du.name LIKE '%' || i_name || '%'))) a;     
        OPEN o_user_list FOR
          SELECT aa.ssic_id,
                 aa.person_id,
                 aa.name,
                 aa.stru_id,
                 aa.stru_sname,
                 aa.work_state_code,
                 aa.duty_lv,
                 pckg_dams_util.func_decode('DUTY_LV', aa.duty_lv, 'DAMS') duty_lv_name,
                 num,
                 ssic_id || '_' || stru_id id
            FROM (SELECT a.ssic_id,
                         a.person_id,
                         a.name,
                         a.stru_id,
                         a.stru_sname,
                         a.work_state_code,
                         a.duty_lv,
                         row_number() over(ORDER BY a.duty_sort DESC NULLS LAST, nvl(a.if_preside_work, '502002'), a.sort DESC NULLS LAST, a.name ASC NULLS LAST) num
                    FROM (SELECT u.ssic_id,
                                 u.person_id,
                                 u.name,
                                 u.stru_id,
                                 b.stru_sname,
                                 u.duty_lv,
                                 u.duty_sort,
                                 u.if_preside_work,
                                 u.sort,
                                 u.work_state work_state_code
                            FROM dams_user            u,
                                 dams_branch_relation br,
                                 dams_branch          b
                           WHERE EXISTS (SELECT 1
                                    FROM dams_gen_dict gd
                                   WHERE gd.dicttype = 'SECGP_DUTY'
                                     AND gd.dictcode = u.duty_lv
                                     AND gd.dictlang = 'zh_CN'
                                     AND gd.sys_code = 'QXSQ')
                             AND u.stru_id = br.bnch_id
                             AND u.stru_id = b.stru_id
                             AND br.up_bnch_id = v_branch_id
                          UNION ALL
                          SELECT u.ssic_id,
                                 u.person_id,
                                 u.name,
                                 u.stru_id,
                                 b.stru_sname,
                                 u.duty_lv,
                                 u.duty_sort,
                                 u.if_preside_work,
                                 u.sort,
                                 u.work_state work_state_code
                            FROM dams_user u, dams_branch b
                           WHERE EXISTS (SELECT 1
                                    FROM dams_gen_dict gd
                                   WHERE gd.dicttype = 'SECGP_DUTY'
                                     AND gd.dictlang = 'zh_CN'
                                     AND gd.dictcode = u.duty_lv)
                             AND u.stru_id = i_branch_id
                             AND u.stru_id = b.stru_id
                          UNION ALL
                          SELECT dubr.ssic_id,
                                 du.person_id,
                                 du.name,
                                 dubr.stru_id,
                                 b.stru_sname,
                                 du.duty_lv,
                                 du.duty_sort,
                                 du.if_preside_work,
                                 du.sort,
                                 du.work_state work_state_code
                            FROM dams_user_branch_rel dubr,
                                 dams_user            du,
                                 dams_branch_relation br,
                                 dams_branch          b
                           WHERE du.ssic_id = dubr.ssic_id
                             AND EXISTS (SELECT 1
                                    FROM dams_gen_dict gd
                                   WHERE gd.dicttype = 'SECGP_DUTY'
                                     AND gd.dictcode = du.duty_lv
                                     AND gd.dictlang = 'zh_CN'
                                     AND gd.sys_code = 'QXSQ')
                             AND dubr.stru_id = br.bnch_id
                             AND br.up_bnch_id = v_branch_id
                             AND dubr.stru_id = b.stru_id
                          UNION ALL
                          SELECT dubr.ssic_id,
                                 du.person_id,
                                 du.name,
                                 dubr.stru_id,
                                 b.stru_sname,
                                 du.duty_lv,
                                 du.duty_sort,
                                 du.if_preside_work,
                                 du.sort,
                                 du.work_state work_state_code
                            FROM dams_user_branch_rel dubr,
                                 dams_user            du,
                                 dams_branch          b
                           WHERE du.ssic_id = dubr.ssic_id
                             AND EXISTS (SELECT 1
                                    FROM dams_gen_dict gd
                                   WHERE gd.dicttype = 'SECGP_DUTY'
                                     AND gd.dictcode = du.duty_lv
                                     AND gd.dictlang = 'zh_CN'
                                     AND gd.sys_code = 'QXSQ')
                             AND du.stru_id = i_branch_id
                             AND dubr.stru_id = b.stru_id) a
                   WHERE i_name IS NULL
                      OR (a.stru_sname LIKE '%' || i_name || '%' OR
                         a.name LIKE '%' || i_name || '%')) aa
           WHERE num < (to_number(i_beg_num) + to_number(i_fetch_num))
             AND num >= to_number(i_beg_num);
        --密码办主任，保密办主任选人逻辑
      WHEN i_sys_code = 'STSECGP2' OR i_sys_code IS NULL THEN
        v_branch_id := pckg_dams_util.func_get_lv2_branch(i_branch_id);      
        SELECT COUNT(1)
          INTO o_total_num
          FROM (SELECT 1
                  FROM dams_user            u,
                       dams_branch_relation br,
                       dams_branch          b,
                       dams_branch_catalog  bc,
                       dams_branch          b2
                 WHERE u.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND u.stru_id = b.stru_id
                   AND u.stru_id = bc.stru_id
                   AND bc.dept_stru_id = b2.stru_id
                   AND (b2.stru_sname LIKE '%办公室' OR b2.stru_sname LIKE '%综合管理部')
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       u.name LIKE '%' || i_name || '%'))
                UNION ALL
                SELECT 1
                  FROM dams_user_branch_rel dubr,
                       dams_user            du,
                       dams_branch_relation br,
                       dams_branch          b,
                       dams_branch_catalog  bc,
                       dams_branch          b2
                 WHERE du.ssic_id = dubr.ssic_id
                   AND du.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND dubr.stru_id = b.stru_id
                   AND dubr.stru_id = bc.stru_id
                   AND bc.dept_stru_id = b2.stru_id
                   AND (b2.stru_sname LIKE '%办公室' OR b2.stru_sname LIKE '%综合管理部')
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       du.name LIKE '%' || i_name || '%'))) a;     
        OPEN o_user_list FOR
          SELECT aa.ssic_id,
                 aa.person_id,
                 aa.name,
                 aa.stru_id,
                 aa.stru_sname,
                 aa.work_state_code,
                 aa.duty_lv,
                 pckg_dams_util.func_decode('DUTY_LV', aa.duty_lv, 'DAMS') duty_lv_name,
                 num,
                 ssic_id || '_' || stru_id id
            FROM (SELECT a.ssic_id,
                         a.person_id,
                         a.name,
                         a.stru_id,
                         a.stru_sname,
                         a.work_state_code,
                         a.duty_lv,
                         row_number() over(ORDER BY a.duty_sort DESC NULLS LAST, nvl(a.if_preside_work, '502002'), a.sort DESC NULLS LAST, a.name ASC NULLS LAST) num
                    FROM (SELECT u.ssic_id,
                                 u.person_id,
                                 u.name,
                                 u.stru_id,
                                 b.stru_sname,
                                 u.duty_lv,
                                 u.duty_sort,
                                 u.if_preside_work,
                                 u.sort,
                                 u.work_state work_state_code
                            FROM dams_user            u,
                                 dams_branch_relation br,
                                 dams_branch          b,
                                 dams_branch_catalog  bc,
                                 dams_branch          b2
                           WHERE u.stru_id = br.bnch_id
                             AND u.stru_id = b.stru_id
                             AND u.stru_id = bc.stru_id
                             AND bc.dept_stru_id = b2.stru_id
                             AND (b2.stru_sname LIKE '%办公室' OR
                                 b2.stru_sname LIKE '%综合管理部')
                             AND br.up_bnch_id = v_branch_id
                          UNION ALL
                          SELECT dubr.ssic_id,
                                 du.person_id,
                                 du.name,
                                 dubr.stru_id,
                                 b.stru_sname,
                                 du.duty_lv,
                                 du.duty_sort,
                                 du.if_preside_work,
                                 du.sort,
                                 du.work_state work_state_code
                            FROM dams_user_branch_rel dubr,
                                 dams_user            du,
                                 dams_branch_relation br,
                                 dams_branch          b,
                                 dams_branch_catalog  bc,
                                 dams_branch          b2
                           WHERE du.ssic_id = dubr.ssic_id
                             AND du.stru_id = bc.stru_id
                             AND bc.dept_stru_id = b2.stru_id
                             AND (b2.stru_sname LIKE '%办公室' OR
                                 b2.stru_sname LIKE '%综合管理部')
                             AND du.stru_id = br.bnch_id
                             AND br.up_bnch_id = v_branch_id
                             AND dubr.stru_id = b.stru_id) a
                   WHERE i_name IS NULL
                      OR (a.stru_sname LIKE '%' || i_name || '%' OR
                         a.name LIKE '%' || i_name || '%')) aa
           WHERE num < (to_number(i_beg_num) + to_number(i_fetch_num))
             AND num >= to_number(i_beg_num);
        --保密委员会成员，密码工作领导小组成员选人逻辑
      WHEN i_sys_code = 'STSECGP3' OR i_sys_code IS NULL THEN
        --v_branch_id := pckg_dams_util.func_get_lv2_branch(i_branch_id);
        v_branch_id := i_branch_id;    
        SELECT COUNT(1)
          INTO o_total_num
          FROM (SELECT 1
                  FROM dams_user u, dams_branch_relation br, dams_branch b
                 WHERE u.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND u.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       u.name LIKE '%' || i_name || '%'))
                UNION ALL
                SELECT 1
                  FROM dams_user_branch_rel dubr,
                       dams_user            du,
                       dams_branch_relation br,
                       dams_branch          b
                 WHERE du.ssic_id = dubr.ssic_id
                   AND du.stru_id = br.bnch_id
                   AND br.up_bnch_id = v_branch_id
                   AND dubr.stru_id = b.stru_id
                   AND (i_name IS NULL OR
                       (b.stru_sname LIKE '%' || i_name || '%' OR
                       du.name LIKE '%' || i_name || '%'))) a;     
        OPEN o_user_list FOR
          SELECT aa.ssic_id,
                 aa.person_id,
                 aa.name,
                 aa.stru_id,
                 aa.stru_sname,
                 aa.work_state_code,
                 aa.duty_lv,
                 pckg_dams_util.func_decode('DUTY_LV', aa.duty_lv, 'DAMS') duty_lv_name,
                 num,
                 ssic_id || '_' || stru_id id
            FROM (SELECT a.ssic_id,
                         a.person_id,
                         a.name,
                         a.stru_id,
                         a.stru_sname,
                         a.work_state_code,
                         a.duty_lv,
                         row_number() over(ORDER BY a.duty_sort DESC NULLS LAST, nvl(a.if_preside_work, '502002'), a.sort DESC NULLS LAST, a.name ASC NULLS LAST) num
                    FROM (SELECT u.ssic_id,
                                 u.person_id,
                                 u.name,
                                 u.stru_id,
                                 b.stru_sname,
                                 u.duty_lv,
                                 u.duty_sort,
                                 u.if_preside_work,
                                 u.sort,
                                 u.work_state work_state_code
                            FROM dams_user            u,
                                 dams_branch_relation br,
                                 dams_branch          b
                           WHERE u.stru_id = br.bnch_id
                             AND u.stru_id = b.stru_id
                             AND br.up_bnch_id = v_branch_id
                          UNION ALL
                          SELECT dubr.ssic_id,
                                 du.person_id,
                                 du.name,
                                 dubr.stru_id,
                                 b.stru_sname,
                                 du.duty_lv,
                                 du.duty_sort,
                                 du.if_preside_work,
                                 du.sort,
                                 du.work_state work_state_code
                            FROM dams_user_branch_rel dubr,
                                 dams_user            du,
                                 dams_branch_relation br,
                                 dams_branch          b
                           WHERE du.ssic_id = dubr.ssic_id
                             AND du.stru_id = br.bnch_id
                             AND br.up_bnch_id = v_branch_id
                             AND dubr.stru_id = b.stru_id) a
                   WHERE i_name IS NULL
                      OR (a.stru_sname LIKE '%' || i_name || '%' OR
                         a.name LIKE '%' || i_name || '%')) aa
           WHERE num < (to_number(i_beg_num) + to_number(i_fetch_num))
             AND num >= to_number(i_beg_num);
      ELSE
        NULL;
    END CASE;
  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := '-1';
      o_msg      := '系统异常！';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_user_choice_list;
  
  PROCEDURE proc_get_role_strus(i_ssic_id  IN VARCHAR2, --用户编码
                                i_role_id  IN VARCHAR2, --角色id
                                o_ret_code OUT VARCHAR2, --返回标志
                                o_msg      OUT VARCHAR2, --返回信息
                                o_count    OUT VARCHAR2, --机构数量
                                o_strus    OUT SYS_REFCURSOR --机构列表
                                ) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_user_choice_list';
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_role_id;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';
    o_msg      := '';
    SELECT COUNT(1)
      INTO o_count
      FROM dams_user_role_rel t
     WHERE t.ssic_id = i_ssic_id
       AND t.role_id = i_role_id
       AND t.auth_state <> '3';
    OPEN o_strus FOR
      SELECT t.stru_id, b.stru_sname stru_name, r.role_name
        FROM dams_user_role_rel t, dams_role_nls r, dams_branch b
       WHERE t.stru_id = b.stru_id
         AND t.ssic_id = i_ssic_id
         AND t.role_id = i_role_id
         AND t.role_id = r.role_id
         AND t.auth_state <> '3';
  EXCEPTION
    WHEN OTHERS THEN
      IF o_strus%ISOPEN THEN
        CLOSE o_strus;
      END IF;
      o_ret_code := '-1';
      o_msg      := '系统异常！';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_get_role_strus;
  
  PROCEDURE proc_get_role_menu(i_ssic_id  IN VARCHAR2,
                               o_ret_code OUT VARCHAR2,
                               o_menus    OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_role_menu';
  BEGIN
    o_ret_code := '0';
    --SELECT '[' || dams_conn('"' || menus || '"') || ']'
    SELECT '[' || '"' || menus || '"' || ']'
      INTO o_menus
      FROM (SELECT DISTINCT da.resource_id menus
              FROM dams_user_role_rel dr, dams_acl da
             WHERE dr.ssic_id = i_ssic_id
               AND dr.role_id = da.principal_id
               AND da.resource_id IN
                   ('M003032', 'M003007', 'M003020', 'M003011', 'M003004',
                    'M003013', 'M003006', 'AD03')
               AND EXISTS
             (SELECT 1
                      FROM dams_branch_sys_rel dsr, dams_branch_relation dbr
                     WHERE dr.stru_id = dbr.bnch_id
                       AND dbr.up_bnch_id = dsr.stru_id
                       AND dsr.sys_code IN ('RB', 'AD', 'SD')
                       AND dsr.start_use = '1'));

  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || i_ssic_id);
  END proc_get_role_menu;
  
  PROCEDURE proc_get_dept_stru(i_stru_id   IN VARCHAR2,
                               o_ret_code  OUT VARCHAR2,
                               o_dept_stru OUT SYS_REFCURSOR) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_dept_stru';
  BEGIN
    o_ret_code := '0';
    OPEN o_dept_stru FOR
      SELECT bc1.dept_stru_id, b.stru_sname
        FROM dams_branch_catalog bc, dams_branch_catalog bc1, dams_branch b
       WHERE bc.stru_id = i_stru_id
         AND bc1.major_stru_id = bc.major_stru_id
         AND bc1.dept_stru_id = bc1.stru_id
         AND bc1.dept_stru_id = b.stru_id
       ORDER BY b.stru_sname ASC NULLS LAST;
  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := '-1';
      IF o_dept_stru%ISOPEN THEN
        CLOSE o_dept_stru;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || i_stru_id);
  END proc_get_dept_stru;
  
  FUNCTION func_getregionno60(in_year_month IN VARCHAR2) RETURN VARCHAR2 IS
    RESULT VARCHAR2(2);
  BEGIN
    IF in_year_month = '999999' THEN
      RETURN '00';
    END IF;
    RESULT := MOD(to_number(substr(in_year_month, 1, 4)), 5) * 12 +
              to_number(substr(in_year_month, 5, 2));
    RESULT := lpad(RESULT, 2, '0');
    RETURN RESULT;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN('');
  END func_getregionno60;
  
  --FUNCTION encrypt_3des(clear_text IN VARCHAR2, --需加密的明文
  --                      key        IN VARCHAR2 --密钥
  --                      ) RETURN VARCHAR2 IS
  --  --encryption_type PLS_INTEGER := dbms_crypto.encrypt_3des + dbms_crypto.chain_ecb + dbms_crypto.pad_pkcs5;
  --  encryption_type PLS_INTEGER := dbms_crypto.chain_ecb + dbms_crypto.pad_pkcs5;
  --BEGIN
  --  RETURN utl_i18n.raw_to_char(utl_encode.base64_encode(dbms_crypto.encrypt(utl_i18n.string_to_raw(clear_text, 'AL32UTF8'), encryption_type, utl_i18n.string_to_raw(key, 'AL32UTF8'))), 'AL32UTF8');
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    RAISE;
  --    RETURN NULL;
  --END encrypt_3des;
  --
  --FUNCTION decrypt_3des(cipher_text IN VARCHAR2, --需加密的明文
  --                      key         IN VARCHAR2 --密钥
  --                      ) RETURN VARCHAR2 IS
  --  --encryption_type PLS_INTEGER := dbms_crypto.encrypt_3des + dbms_crypto.chain_ecb + dbms_crypto.pad_pkcs5;
  --  encryption_type PLS_INTEGER := dbms_crypto.chain_ecb + dbms_crypto.pad_pkcs5;
  --BEGIN
  --  RETURN utl_i18n.raw_to_char(dbms_crypto.decrypt(utl_encode.base64_decode(utl_i18n.string_to_raw(cipher_text, 'AL32UTF8')), encryption_type, utl_i18n.string_to_raw(key, 'AL32UTF8')), 'AL32UTF8');
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    RETURN NULL;
  --END decrypt_3des;
  --
  --FUNCTION des(i_sign IN VARCHAR2, --秘钥标识
  --             i_text IN VARCHAR2 --
  --             ) RETURN VARCHAR2 IS
  --BEGIN
  --  IF i_text IS NULL THEN
  --    RETURN '';
  --  ELSE
  --    RETURN pckg_dams_util.encrypt_3des(i_text, pckg_dams_util.getkey(i_sign));
  --  END IF;
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    RAISE;
  --    RETURN NULL;
  --END des;
  --
  --FUNCTION getkey(i_sign IN VARCHAR2 --秘钥标识
  --                ) RETURN VARCHAR2 IS
  --  v_key VARCHAR2(24) := '000000000000000000000000';
  --BEGIN
  --  SELECT substr(rpad(pckg_dams_util.encrypt_3des(i_sign, t.dictvalue), 24, '='), 1, 24)
  --    INTO v_key
  --    FROM dams_gen_dict t
  --   WHERE t.dicttype = '3DESGENKEY'
  --     AND t.dictcode = 'GEN4BIN'
  --     AND t.dictlang = 'zh_CN'
  --     AND t.sys_code = 'OIS';
  --  RETURN v_key;
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    RETURN NULL;
  --END getkey;
  --
  --FUNCTION getkey(i_sign     IN VARCHAR2, --秘钥标识
  --                i_dictcode IN VARCHAR2,
  --                i_sys_code IN VARCHAR2) RETURN VARCHAR2 IS
  --  v_key VARCHAR2(24) := '000000000000000000000000';
  --BEGIN
  --
  --  SELECT substr(rpad(pckg_dams_util.encrypt_3des(i_sign, t.dictvalue), 24, '='), 1, 24)
  --    INTO v_key
  --    FROM dams_gen_dict t
  --   WHERE t.dicttype = '3DESGENKEY'
  --     AND t.dictcode = nvl(i_dictcode, 'GEN4BIN')
  --     AND t.dictlang = 'zh_CN'
  --     AND t.sys_code = i_sys_code; 
  --  RETURN v_key;
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    RETURN NULL;
  --END getkey;
  
END pckg_dams_util;
//






CREATE OR REPLACE PACKAGE pckg_dams_dict_ref IS
  PROCEDURE proc_dict_query_bycode(i_begnum            IN VARCHAR2, --开始行
                                   i_fetchnum          IN VARCHAR2, --取得行数
                                   i_dictlang          IN VARCHAR2, --系统语言
                                   i_syscode           IN VARCHAR2, --系统code
                                   i_dicttype          IN VARCHAR2, --字典类型
                                   i_excludesjfilespec IN VARCHAR2, --是否移除SJFILES_SPEC
                                   o_flag              OUT VARCHAR2, --0  成功  1 取信息错
                                   o_totalnum          OUT VARCHAR2, --记录条数
                                   o_dictlist          OUT SYS_REFCURSOR --通用字典表查询结果集
                                   );
  
  PROCEDURE proc_dict_get_sys_list(o_flag    OUT VARCHAR2, --存储过程返回标志
                                   o_syslist OUT SYS_REFCURSOR --返回子系统列表
                                   );
  
  PROCEDURE proc_dict_get_dicttype_list(i_syscode           IN VARCHAR2, --系统编号
                                        i_excludesjfilespec IN VARCHAR2, --是否移除SJFILES_SPEC
                                        o_flag              OUT VARCHAR2, --存储过程返回标志
                                        o_dicttypelist      OUT SYS_REFCURSOR --返回字典类型列表
                                        );
  
  --PROCEDURE proc_refresh_mview(i_table_name IN VARCHAR2, --表名
  --                             i_method     IN VARCHAR2, --刷新方式fast/complete
  --                             o_flag       OUT VARCHAR2 --存储过程返回标志
  --                             );
  
  PROCEDURE proc_dict_adddict(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              );
  
  PROCEDURE proc_dict_deldict(i_dicttype    IN VARCHAR2, --字典类型
                              i_dictcode    IN VARCHAR2, --字典编码
                              i_dictlang    IN VARCHAR2, --语言
                              i_sys_code    IN VARCHAR2, --系统编号
                              i_parent_code IN VARCHAR2, --父类型编码
                              i_op_user     IN VARCHAR2, --操作人
                              o_flag        OUT VARCHAR2 --存储过程返回标志
                              );
  
  PROCEDURE proc_dict_moddict(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictcode       IN VARCHAR2, --字典类型编码
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_defaultvalue   IN VARCHAR2, --字典默认值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              );
  
  PROCEDURE proc_get_dict_list(i_dictlang    IN VARCHAR2, --系统语言
                               i_sys_code    IN VARCHAR2, --系统code
                               i_dicttype    IN VARCHAR2, --字典类型
                               i_parent_code IN VARCHAR2,
                               o_ret_code    OUT VARCHAR2, --0  成功  -1 取信息错
                               o_total_num   OUT VARCHAR2, --记录条数
                               o_dict_list   OUT SYS_REFCURSOR --通用字典表查询结果集
                               );
  
  PROCEDURE proc_add_biz_line(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              );
  
  PROCEDURE proc_dict_edit_order(i_dictlang  IN VARCHAR2, --系统语言
                                 i_sys_code  IN VARCHAR2, --系统code
                                 i_dicttype  IN VARCHAR2, --字典类型
                                 i_dictcodes IN VARCHAR2,
                                 o_ret_code  OUT VARCHAR2, --0  成功
                                 o_msg       OUT VARCHAR2 --
                                 );
  
END pckg_dams_dict_ref;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_dict_ref IS
  PROCEDURE proc_dict_query_bycode(i_begnum            IN VARCHAR2, --开始行
                                   i_fetchnum          IN VARCHAR2, --取得行数
                                   i_dictlang          IN VARCHAR2, --系统语言
                                   i_syscode           IN VARCHAR2, --系统code
                                   i_dicttype          IN VARCHAR2, --字典类型
                                   i_excludesjfilespec IN VARCHAR2, --是否移除SJFILES_SPEC
                                   o_flag              OUT VARCHAR2, --0  成功  -1 取信息错
                                   o_totalnum          OUT VARCHAR2, --记录条数
                                   o_dictlist          OUT SYS_REFCURSOR --通用字典表查询结果集
                                   ) IS
    v_sql  VARCHAR2(2000);
    v_sql2 VARCHAR2(2000);
  BEGIN
    o_flag := '0';
    IF i_excludesjfilespec = '1' THEN
      OPEN o_dictlist FOR
        SELECT dicttype,
               dictdes,
               dictcode,
               dictvalue,
               defaultvalue,
               canmodify_code,
               dictdescript,
               dictlang,
               dictlangvalue,
               parent_code,
               --'' parent_code_name,
               sys_code,
               sys_code_name,
               valid_flag,
               rn row_num
          FROM (SELECT t1.dicttype,
                       t2.dictdescript dictdes,
                       t1.dictcode,
                       t1.dictvalue,
                       t1.defaultvalue,
                       t1.canmodify_code,
                       t1.dictdescript,
                       t1.dictlang,
                       CASE t1.dictlang
                         WHEN 'zh_CN' THEN
                          '中文'
                         WHEN 'en_US' THEN
                          '英文'
                       END dictlangvalue,
                       t1.parent_code,
                       t1.sys_code,
                       (SELECT sys_name
                          FROM dams_sys_conf
                         WHERE sys_code = t1.sys_code) sys_code_name,
                       t1.valid_flag,
                       row_number() over(ORDER BY t1.dicttype, t1.dictcode) rn
                  FROM dams_gen_dict t1, dams_gen_dict_ref t2
                 WHERE t1.sys_code = t2.sys_code
                   AND t1.dicttype = t2.dicttype
                   AND (t2.dicttype <> 'SJFILE_SPEC')
                   AND t2.canmodify_code = '1'
                   AND t2.valid_flag = '1'
                      --AND t1.canmodify_code = '1'
                   AND t1.valid_flag = '1'
                   AND t1.sys_code = nvl(i_syscode, t1.sys_code)
                   AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
                   AND t1.dicttype = nvl(i_dicttype, t1.dicttype))
         WHERE rn >= to_number(i_begnum)
           AND rn < to_number(i_begnum) + to_number(i_fetchnum);
      SELECT COUNT(1)
        INTO o_totalnum
        FROM dams_gen_dict t1, dams_gen_dict_ref t2
       WHERE t1.sys_code = t2.sys_code
         AND t1.dicttype = t2.dicttype
         AND (t2.dicttype <> 'SJFILE_SPEC')
         AND t2.canmodify_code = '1'
         AND t2.valid_flag = '1'
            --AND t1.canmodify_code = '1'
         AND t1.valid_flag = '1'
         AND t1.sys_code = nvl(i_syscode, t1.sys_code)
         AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
         AND t1.dicttype = nvl(i_dicttype, t1.dicttype);
    ELSE
      OPEN o_dictlist FOR
        SELECT dicttype,
               dictdes,
               dictcode,
               dictvalue,
               defaultvalue,
               canmodify_code,
               dictdescript,
               dictlang,
               dictlangvalue,
               parent_code,
               sys_code,
               sys_code_name,
               valid_flag,
               rn row_num
          FROM (SELECT t1.dicttype,
                       t2.dictdescript dictdes,
                       t1.dictcode,
                       t1.dictvalue,
                       t1.defaultvalue,
                       t1.canmodify_code,
                       t1.dictdescript,
                       t1.dictlang,
                       CASE t1.dictlang
                         WHEN 'zh_CN' THEN
                          '中文'
                         WHEN 'en_US' THEN
                          '英文'
                       END dictlangvalue,
                       t1.parent_code,
                       t1.sys_code,
                       (SELECT sys_name
                          FROM dams_sys_conf
                         WHERE sys_code = t1.sys_code) sys_code_name,
                       t1.valid_flag,
                       row_number() over(ORDER BY t1.dicttype, t1.dictcode) rn
                  FROM dams_gen_dict t1, dams_gen_dict_ref t2
                 WHERE t1.sys_code = t2.sys_code
                   AND t1.dicttype = t2.dicttype
                   AND t2.canmodify_code = '1'
                   AND t2.valid_flag = '1'
                      --AND t1.canmodify_code = '1'
                   AND t1.valid_flag = '1'
                   AND t1.sys_code = nvl(i_syscode, t1.sys_code)
                   AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
                   AND t1.dicttype = nvl(i_dicttype, t1.dicttype))
         WHERE rn >= to_number(i_begnum)
           AND rn < to_number(i_begnum) + to_number(i_fetchnum);
      SELECT COUNT(1)
        INTO o_totalnum
        FROM dams_gen_dict t1, dams_gen_dict_ref t2
       WHERE t1.sys_code = t2.sys_code
         AND t1.dicttype = t2.dicttype
         AND t2.canmodify_code = '1'
         AND t2.valid_flag = '1'
            --AND t1.canmodify_code = '1'
         AND t1.valid_flag = '1'
         AND t1.sys_code = nvl(i_syscode, t1.sys_code)
         AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
         AND t1.dicttype = nvl(i_dicttype, t1.dicttype);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_QUERY_BYCODE()', SQLERRM(SQLCODE));
      o_flag := '-1';
      --需要主动关闭游标
      IF o_dictlist%ISOPEN THEN
        CLOSE o_dictlist;
      END IF;
      RETURN;
  END proc_dict_query_bycode;
  
  PROCEDURE proc_dict_get_sys_list(o_flag    OUT VARCHAR2, --存储过程返回标志
                                   o_syslist OUT SYS_REFCURSOR --返回子系统列表
                                   ) IS
  BEGIN
    o_flag := '0';
    OPEN o_syslist FOR
      SELECT t.sys_code, t.sys_name FROM dams_sys_conf t;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_GET_SYS_LIST()', SQLERRM(SQLCODE));
      o_flag := '-1';
      IF o_syslist%ISOPEN THEN
        CLOSE o_syslist;
      END IF;
      RETURN;
  END proc_dict_get_sys_list;
  
  PROCEDURE proc_dict_get_dicttype_list(i_syscode           IN VARCHAR2, --系统编号
                                        i_excludesjfilespec IN VARCHAR2, --是否移除SJFILES_SPEC
                                        o_flag              OUT VARCHAR2, --存储过程返回标志
                                        o_dicttypelist      OUT SYS_REFCURSOR --返回字典类型列表
                                        ) IS
  BEGIN
    o_flag := '0';
    IF i_excludesjfilespec = '1' THEN
      OPEN o_dicttypelist FOR
        SELECT dicttype dicttypecd, dictdescript dicttypename
          FROM dams_gen_dict_ref
         WHERE canmodify_code = '1'
           AND valid_flag = '1'
           AND dicttype <> 'SJFILE_SPEC'
           AND sys_code = i_syscode;
    ELSE
      OPEN o_dicttypelist FOR
        SELECT dicttype dicttypecd, dictdescript dicttypename
          FROM dams_gen_dict_ref
         WHERE canmodify_code = '1'
           AND valid_flag = '1'
           AND sys_code = i_syscode;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_GET_DICTTYPE_LIST()', SQLERRM(SQLCODE));
      o_flag := '-1';
      IF o_dicttypelist%ISOPEN THEN
        CLOSE o_dicttypelist;
      END IF;
      RETURN;
  END proc_dict_get_dicttype_list;
  
  PROCEDURE proc_dict_get_pratype_list(i_dicttype    IN VARCHAR2, --字典类型
                                       i_syscode     IN VARCHAR2, --系统编号
                                       o_flag        OUT VARCHAR2, --存储过程返回标志
                                       o_pratypelist OUT SYS_REFCURSOR --返回父字典类型编码列表
                                       ) IS
  BEGIN
    o_flag := '0';
    OPEN o_pratypelist FOR
      SELECT DISTINCT t1.dictcode prarentcd, t1.dictvalue prarentname
        FROM dams_gen_dict t1
       WHERE t1.valid_flag = '1'
         AND t1.dicttype = (SELECT t2.parenttype
                              FROM dams_gen_dict_ref t2
                             WHERE t2.sys_code = i_syscode
                               AND t2.dicttype = i_dicttype
                               AND t2.valid_flag = '1')
         AND t1.sys_code = i_syscode
         AND t1.dictlang = 'zh_CN'
       ORDER BY t1.dictcode;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_GET_PRATYPE_LIST()', SQLERRM(SQLCODE));
      o_flag := '-1';
      IF o_pratypelist%ISOPEN THEN
        CLOSE o_pratypelist;
      END IF;
      RETURN;
  END proc_dict_get_pratype_list;
  
  --PROCEDURE proc_refresh_mview(i_table_name IN VARCHAR2, --表名
  --                             i_method     IN VARCHAR2, --刷新方式fast/complete
  --                             o_flag       OUT VARCHAR2 --存储过程返回标志
  --                             ) IS
  --  v_param     db_log.info%TYPE := i_table_name || '|' || i_method;
  --  v_proc_name db_log.proc_name%TYPE := 'pckg_dams_dict_ref.proc_refresh_mview';
  --
  --BEGIN
  --  o_flag := '0';
  --  dbms_mview.refresh(upper(i_table_name), upper(i_method));
  --
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --    o_flag := '-1';
  --    ROLLBACK;
  --END proc_refresh_mview;
  
  PROCEDURE proc_dict_adddict(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              ) IS
    v_dictcode     VARCHAR2(30); --最大的字典编码
    v_old_length   NUMBER; --字典编码的旧的长度
    v_new_dictcode VARCHAR2(30); --新的字典编码
    v_menu_id      VARCHAR2(30); --菜单编号
    v_menu_count   NUMBER; --菜单个数
    v_url          VARCHAR2(100); --菜单URL
    v_menu_id2     VARCHAR2(30); --菜单编号
    v_url2         VARCHAR2(100); --菜单URL
    v_totalnum     NUMBER;
    v_logobj     dams_gen_dict.dictcode%TYPE := 'SYSPARAM_OPERATE';
    v_logtype    VARCHAR2(3) := '011';
    v_logdate    VARCHAR2(14);
    v_loghandler dams_user.ssic_id%TYPE;
    v_loginfo VARCHAR2(4000);
    v_temp    VARCHAR2(4000);
  BEGIN
    --SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logdate FROM dual;
	SELECT to_char(to_date('20210122104949','yyyymmddhh24miss'), 'yyyymmddhh24miss') INTO v_logdate FROM dual;
    IF i_sys_code = 'SJ' THEN
      v_logtype := '012';
    END IF;
    o_flag := '0';
    SELECT lpad(nvl(MAX(to_number(regexp_substr(dictcode, '[0-9]+'))), 0) + 1, 5,
                '0')
      INTO v_new_dictcode
      FROM dams_gen_dict
     WHERE dicttype = i_dicttype
       AND dictlang = i_dictlang
       AND sys_code = i_sys_code
       AND regexp_substr(dictcode, '[0-9]+') IS NOT NULL;
    --V_NEW_DICTCODE := TO_CHAR(V_TOTALNUM + 1);
    --改为取表中现有最大记录+1为新字典值 mod by xiesy 20130411
    INSERT INTO ois_gen_dict
      (dicttype,
       dictcode,
       dictvalue,
       defaultvalue,
       canmodify_code,
       dictdescript,
       dictlang,
       parent_code,
       sys_code,
       valid_flag)
    VALUES
      (i_dicttype,
       v_new_dictcode,
       i_dictvalue,
       '',
       i_canmodify_code,
       i_dictdescript,
       i_dictlang,
       i_parent_code,
       i_sys_code,
       i_valid_flag);
    SELECT MAX(t.dictdescript)
      INTO v_temp
      FROM dams_gen_dict_ref t
     WHERE t.dicttype = i_dicttype
       AND t.sys_code = i_sys_code;
    v_loginfo := '新增字典类型:' || v_temp;
    v_loginfo := v_loginfo || ' 字典项编码:' || v_new_dictcode || ' ';
    v_loginfo := v_loginfo || ' 字典值:' || i_dictvalue;
    SELECT MAX(t.sys_sname)
      INTO v_temp
      FROM dams_sys_conf t
     WHERE t.sys_code = i_sys_code;
    v_loginfo := v_loginfo || ' 所属系统:' || v_temp || ';';
    pckg_dams_util.proc_dams_logger(v_logobj, v_logtype, v_logdate, i_op_user,
                                    v_loginfo);
    COMMIT;
    --pckg_dams_dict_ref.proc_refresh_mview('DAMS_GEN_DICT', 'COMPLETE', o_flag);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_ADDDICT()', SQLERRM(SQLCODE));
      o_flag := '-1';
      ROLLBACK;
  END proc_dict_adddict;
  
  PROCEDURE proc_dict_deldict(i_dicttype    IN VARCHAR2, --字典类型
                              i_dictcode    IN VARCHAR2, --字典编码
                              i_dictlang    IN VARCHAR2, --语言
                              i_sys_code    IN VARCHAR2, --系统编号
                              i_parent_code IN VARCHAR2, --父类型编码
                              i_op_user     IN VARCHAR2, --操作人
                              o_flag        OUT VARCHAR2 --存储过程返回标志
                              ) IS

    v_count    NUMBER;
    v_count1   NUMBER;
    v_menu_id  VARCHAR2(10);
    v_menu_id2 VARCHAR2(10);
    v_logobj     dams_gen_dict.dictcode%TYPE := 'SYSPARAM_OPERATE';
    v_logtype    VARCHAR2(3) := '011';
    v_logdate    VARCHAR2(14);
    v_loghandler dams_user.ssic_id%TYPE;
    v_loginfo VARCHAR2(4000);
    v_temp    VARCHAR2(4000);
  BEGIN
    --SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logdate FROM dual;
	SELECT to_char(to_date('20210122140320','yyyymmddhh24miss'), 'yyyymmddhh24miss') INTO v_logdate FROM dual;
    IF i_sys_code = 'SJ' THEN
      v_logtype := '012';
    END IF;
    o_flag := '0';
    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT *
              FROM dams_gen_dict
             WHERE dicttype IN (SELECT dicttype
                                  FROM dams_gen_dict_ref
                                 WHERE parenttype = i_dicttype
                                   AND sys_code = i_sys_code)
               AND dictlang = i_dictlang)
     WHERE parent_code = i_dictcode;
    SELECT COUNT(1)
      INTO v_count1
      FROM ois_gen_dict t
     WHERE t.dicttype = i_dicttype
       AND t.dictlang = i_dictlang
       AND t.sys_code = i_sys_code
       AND t.parent_code = t.dictcode
       AND t.valid_flag = '1';
    IF v_count = 0 AND v_count1 = 0 THEN
      UPDATE ois_gen_dict
         SET valid_flag = '0'
       WHERE dicttype = i_dicttype
         AND dictcode = i_dictcode
         AND dictlang = i_dictlang
         AND sys_code = i_sys_code;
      SELECT MAX(t.dictdescript)
        INTO v_temp
        FROM dams_gen_dict_ref t
       WHERE t.dicttype = i_dicttype
         AND t.sys_code = i_sys_code;
      v_loginfo := '删除字典类型:' || v_temp;
      v_loginfo := v_loginfo || ' 字典项编码:' || i_dictcode || ' ';
      SELECT MAX(t.dictvalue)
        INTO v_temp
        FROM dams_gen_dict t
       WHERE t.dicttype = i_dicttype
         AND t.dictcode = i_dictcode
         AND t.dictlang = i_dictlang
         AND t.sys_code = i_sys_code;
      v_loginfo := v_loginfo || ' 字典值:' || v_temp;
      SELECT MAX(t.sys_sname)
        INTO v_temp
        FROM dams_sys_conf t
       WHERE t.sys_code = i_sys_code;
      v_loginfo := v_loginfo || ' 所属系统:' || v_temp || ';';
      IF i_sys_code = 'QXSQ' AND i_dicttype = 'SECDEV_MODEL_TYPE' THEN
        DELETE FROM dams_price_dict t
         WHERE t.typecode = i_dictcode
           AND t.parentcode = i_parent_code;
        v_loginfo := v_loginfo || ' 删除型号价格字典数据:设备类型为' || i_parent_code ||
                     '下的型号为' || i_dictcode || '价格字典数据';
      END IF;
      pckg_dams_util.proc_dams_logger(v_logobj, v_logtype, v_logdate, i_op_user, v_loginfo);
      COMMIT;
      --pckg_dams_dict_ref.proc_refresh_mview('DAMS_GEN_DICT', 'COMPLETE', o_flag);
      COMMIT;
    ELSE
      o_flag := '-2'; --被引用了不能修改
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_DELDICT()', SQLERRM(SQLCODE));
      o_flag := '-1';
      ROLLBACK;
  END proc_dict_deldict;
  
  PROCEDURE proc_dict_moddict(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictcode       IN VARCHAR2, --字典类型编码
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_defaultvalue   IN VARCHAR2, --字典默认值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              ) IS
    v_countnum NUMBER;
    v_count    NUMBER;
    v_menu_id  VARCHAR2(10);
    v_logobj     dams_gen_dict.dictcode%TYPE := 'SYSPARAM_OPERATE';
    v_logtype    VARCHAR2(3) := '011';
    v_logdate    VARCHAR2(14);
    v_loghandler dams_user.ssic_id%TYPE;
    v_loginfo  VARCHAR2(4000);
    v_temp     VARCHAR2(4000);
    v_oldvalue VARCHAR2(4000);
  BEGIN
    --SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logdate FROM dual;
	SELECT to_char(TO_DATE('20210122151351','yyyymmddhh24miss'), 'yyyymmddhh24miss') INTO v_logdate FROM dual;
    IF i_sys_code = 'SJ' THEN
      v_logtype := '012';
    END IF;
    o_flag := '0';
    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT *
              FROM dams_gen_dict
             WHERE dicttype IN (SELECT dicttype
                                  FROM dams_gen_dict_ref
                                 WHERE parenttype = i_dicttype
                                   AND sys_code = i_sys_code)
               AND dictlang = i_dictlang)
     WHERE parent_code = i_dictcode;
    IF v_count = 0 THEN
      SELECT COUNT(*)
        INTO v_countnum
        FROM dams_gen_dict
       WHERE dicttype = i_dicttype
         AND dictcode = i_dictcode
         AND dictlang = i_dictlang
         AND sys_code = i_sys_code;
      IF v_countnum = 1 THEN
        SELECT MAX(t.dictvalue)
          INTO v_oldvalue
          FROM dams_gen_dict t
         WHERE t.dicttype = i_dicttype
           AND t.dictcode = i_dictcode
           AND t.dictlang = i_dictlang
           AND t.sys_code = i_sys_code;
        UPDATE ois_gen_dict
           SET dictvalue      = i_dictvalue,
               defaultvalue   = i_defaultvalue,
               canmodify_code = i_canmodify_code,
               dictdescript   = i_dictdescript,
               --parent_code    = i_parent_code,
               valid_flag = i_valid_flag
         WHERE dicttype = i_dicttype
           AND dictcode = i_dictcode
           AND dictlang = i_dictlang
           AND sys_code = i_sys_code;
        SELECT MAX(t.dictdescript)
          INTO v_temp
          FROM dams_gen_dict_ref t
         WHERE t.dicttype = i_dicttype
           AND t.sys_code = i_sys_code;
        v_loginfo := '修改字典类型:' || v_temp;
        v_loginfo := v_loginfo || ' 字典项编码:' || i_dictcode || ' ';
        v_loginfo := v_loginfo || ' 新字典值:' || i_dictvalue;
        v_loginfo := v_loginfo || ' 原字典值:' || v_oldvalue;
        SELECT MAX(t.sys_sname)
          INTO v_temp
          FROM dams_sys_conf t
         WHERE t.sys_code = i_sys_code;
        v_loginfo := v_loginfo || ' 所属系统:' || v_temp || ';';
        pckg_dams_util.proc_dams_logger(v_logobj, v_logtype, v_logdate, i_op_user, v_loginfo);
        COMMIT;
        --pckg_dams_dict_ref.proc_refresh_mview('DAMS_GEN_DICT', 'COMPLETE', o_flag);
        COMMIT;
      END IF;
    ELSE
      o_flag := '-2'; --被引用了不能修改
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pckg_ctp_lg_public.log('PROC_DICT_MODDICT()', SQLERRM(SQLCODE));
      o_flag := '-1';
      ROLLBACK;
  END proc_dict_moddict;
  
  PROCEDURE proc_get_dict_list(i_dictlang    IN VARCHAR2, --系统语言
                               i_sys_code    IN VARCHAR2, --系统code
                               i_dicttype    IN VARCHAR2, --字典类型
                               i_parent_code IN VARCHAR2,
                               o_ret_code    OUT VARCHAR2, --0  成功  -1 取信息错
                               o_total_num   OUT VARCHAR2, --记录条数
                               o_dict_list   OUT SYS_REFCURSOR --通用字典表查询结果集
                               ) IS
    v_param     db_log.info%TYPE := i_dictlang || '|' || i_sys_code || '|' || i_dicttype || '|' || i_parent_code;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_dict_ref.proc_get_dict_list';
  BEGIN
    o_ret_code := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_gen_dict t1, dams_gen_dict_ref t2
     WHERE t1.sys_code = t2.sys_code
       AND t1.dicttype = t2.dicttype
       AND t2.canmodify_code = '1'
       AND t2.valid_flag = '1'
       AND t1.valid_flag = '1'
       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
       AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
       AND t1.dicttype = nvl(i_dicttype, t1.dicttype)
       AND ((i_parent_code IS NULL AND t1.parent_code IS NULL) OR
           (t1.parent_code = i_parent_code));
    OPEN o_dict_list FOR
      SELECT dicttype,
             dictdes,
             dictcode,
             dictvalue,
             defaultvalue,
             canmodify_code,
             dictdescript,
             dictlang,
             dictlangvalue,
             parent_code,
             sys_code,
             sys_code_name,
             valid_flag,
             rn row_num
        FROM (SELECT t1.dicttype,
                     t2.dictdescript dictdes,
                     t1.dictcode,
                     t1.dictvalue,
                     t1.defaultvalue,
                     t1.canmodify_code,
                     t1.dictdescript,
                     t1.dictlang,
                     CASE t1.dictlang
                       WHEN 'zh_CN' THEN
                        '中文'
                       WHEN 'en_US' THEN
                        '英文'
                     END dictlangvalue,
                     t1.parent_code,
                     t1.sys_code,
                     (SELECT sys_name
                        FROM dams_sys_conf
                       WHERE sys_code = t1.sys_code) sys_code_name,
                     t1.valid_flag,
                     row_number() over(ORDER BY t1.order_no, to_number(t1.dictcode)) rn
                FROM dams_gen_dict t1, dams_gen_dict_ref t2
               WHERE t1.sys_code = t2.sys_code
                 AND t1.dicttype = t2.dicttype
                 AND t2.canmodify_code = '1'
                 AND t2.valid_flag = '1'
                 AND t1.valid_flag = '1'
                 AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                 AND t1.dictlang = nvl(i_dictlang, t1.dictlang)
                 AND t1.dicttype = nvl(i_dicttype, t1.dicttype)
                 AND ((i_parent_code IS NULL AND t1.parent_code IS NULL) OR
                     (t1.parent_code = i_parent_code)));
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      --需要主动关闭游标
      IF o_dict_list%ISOPEN THEN
        CLOSE o_dict_list;
      END IF;
      RETURN;
  END proc_get_dict_list;
  
  PROCEDURE proc_add_biz_line(i_dicttype       IN VARCHAR2, --字典类型
                              i_dictvalue      IN VARCHAR2, --字典值
                              i_canmodify_code IN VARCHAR2, --是否可修改
                              i_dictdescript   IN VARCHAR2, --描述
                              i_dictlang       IN VARCHAR2, --语言
                              i_parent_code    IN VARCHAR2, --字典类型父编码
                              i_sys_code       IN VARCHAR2, --系统编号
                              i_valid_flag     IN VARCHAR2, --有效标志
                              i_op_user        IN VARCHAR2, --操作人
                              o_flag           OUT VARCHAR2 --存储过程返回标志
                              ) IS
    v_param     db_log.info%TYPE := i_dicttype || '|' || i_dictvalue || '|' ||
                                    i_canmodify_code || '|' || i_dictdescript || '|' ||
                                    i_dictlang || '|' || i_parent_code || '|' ||
                                    i_sys_code || '|' || i_valid_flag || '|' ||
                                    i_op_user;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_dict_ref.proc_add_biz_line';
    v_count NUMBER;
    v_dictcode     VARCHAR2(30); --最大的字典编码
    v_order_no     NUMBER;
    v_new_dictcode VARCHAR2(30); --新的字典编码
    v_new_order_no NUMBER;
    v_length NUMBER;
    v_code_1 NUMBER;
    v_code_2 NUMBER;
    v_logobj     dams_gen_dict.dictcode%TYPE := 'SYSPARAM_OPERATE';
    v_logtype    VARCHAR2(3) := '011';
    v_logdate    VARCHAR2(14);
    v_loghandler dams_user.ssic_id%TYPE;
    v_loginfo VARCHAR2(4000);
    v_temp    VARCHAR2(4000);
  BEGIN
    SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logdate FROM dual;
    o_flag := '0';
    SELECT MAX(t.dictcode) keep(dense_rank FIRST ORDER BY t.code_no DESC)
      INTO v_dictcode
      FROM (SELECT d.dictcode, to_number(d.dictcode) code_no
              FROM ois_gen_dict d
             WHERE d.dicttype = i_dicttype
               AND d.dictlang = i_dictlang
               AND d.sys_code = i_sys_code
               AND ((i_parent_code IS NULL AND d.parent_code IS NULL) OR
                   (d.parent_code = i_parent_code))) t;
    SELECT MAX(t.order_no)
      INTO v_order_no
      FROM ois_gen_dict t
     WHERE t.dicttype = i_dicttype
       AND t.dictlang = i_dictlang
       AND t.sys_code = i_sys_code
       AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
           (t.parent_code = i_parent_code));
    IF v_order_no IS NULL THEN
      v_new_order_no := 1;
    ELSE
      IF v_order_no + 1 >= 1000 THEN
        SELECT COUNT(1)
          INTO v_count
          FROM ois_gen_dict t
         WHERE t.dicttype = i_dicttype
           AND t.dictlang = i_dictlang
           AND t.sys_code = i_sys_code
           AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
               (t.parent_code = i_parent_code))
           AND t.valid_flag = '1';
        IF v_count = v_order_no THEN
          v_new_order_no := v_order_no;
        ELSE
          UPDATE ois_gen_dict t
             SET t.order_no = NULL
           WHERE t.dicttype = i_dicttype
             AND t.dictlang = i_dictlang
             AND t.sys_code = i_sys_code
             AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
                 (t.parent_code = i_parent_code))
             AND t.valid_flag = '0';
          UPDATE ois_gen_dict t
             SET t.order_no =
                 (SELECT r.rank
                    FROM (SELECT d.dictcode,
                                 rank() over(PARTITION BY d.dicttype ORDER BY d.order_no) rank
                            FROM ois_gen_dict d
                           WHERE d.dicttype = i_dicttype
                             AND d.dictlang = i_dictlang
                             AND d.sys_code = i_sys_code
                             AND ((i_parent_code IS NULL AND
                                 d.parent_code IS NULL) OR
                                 (d.parent_code = i_parent_code))
                             AND d.valid_flag = '1') r
                   WHERE r.dictcode = t.dictcode)
           WHERE t.dicttype = i_dicttype
             AND t.dictlang = i_dictlang
             AND t.sys_code = i_sys_code
             AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
                 (t.parent_code = i_parent_code))
             AND t.valid_flag = '1';
          SELECT MAX(t.order_no)
            INTO v_order_no
            FROM ois_gen_dict t
           WHERE t.dicttype = i_dicttype
             AND t.dictlang = i_dictlang
             AND t.sys_code = i_sys_code
             AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
                 (t.parent_code = i_parent_code));
          IF v_order_no + 1 >= 1000 THEN
            v_new_order_no := v_order_no;
          ELSE
            v_new_order_no := v_order_no + 1;
          END IF;
        END IF;
      ELSE
        v_new_order_no := v_order_no + 1;
      END IF;
    END IF;
    IF v_dictcode IS NULL THEN
      IF i_parent_code IS NULL THEN
        --一级目录
        v_new_dictcode := '01000';
      ELSE
        --二级目录
        v_length       := length(i_parent_code);
        v_code_1       := to_number(substr(i_parent_code, 1, (v_length - 1) / 2)); --前缀，一级字典使用
        v_code_2       := to_number(substr(i_parent_code, (v_length + 1) / 2,
                                           --后缀，二级字典使用
                                           (v_length + 1) / 2));
        v_new_dictcode := lpad(v_code_1, ((v_length - 1) / 2), '0') ||
                          lpad(v_code_2 + 1, ((v_length + 1) / 2), '0');
      END IF;
    ELSE
      v_length := length(v_dictcode);
      v_code_1 := to_number(substr(v_dictcode, 1, (v_length - 1) / 2)); --前缀，一级字典使用
      v_code_2 := to_number(substr(v_dictcode, (v_length + 1) / 2,
                                   --后缀，二级字典使用
                                   (v_length + 1) / 2));
      IF i_parent_code IS NULL THEN
        --一级字典
        IF v_code_1 + 1 >= power(10, (v_length - 1) / 2) THEN
          --前缀溢出
          v_new_dictcode := lpad(v_code_1 + 1, ((v_length - 1) / 2) + 1, '0') ||
                            lpad('0', ((v_length + 1) / 2) + 1, '0');
        ELSE
          v_new_dictcode := lpad(v_code_1 + 1, ((v_length - 1) / 2), '0') ||
                            lpad('0', ((v_length + 1) / 2), '0');
        END IF;
      ELSE
        --二级字典
        IF v_code_2 + 1 >= power(10, (v_length + 1) / 2) THEN
          --后缀溢出
          v_new_dictcode := lpad(v_code_1, ((v_length - 1) / 2) + 1, '0') ||
                            lpad(v_code_2 + 1, ((v_length + 1) / 2) + 1, '0');
        ELSE
          v_new_dictcode := lpad(v_code_1, ((v_length - 1) / 2), '0') ||
                            lpad(v_code_2 + 1, ((v_length + 1) / 2), '0');
        END IF;
      END IF;
    END IF;
    IF length(v_new_dictcode) > 20 THEN
      o_flag := -3;
      RETURN;
    END IF;
    INSERT INTO ois_gen_dict
      (dicttype,
       dictcode,
       dictvalue,
       defaultvalue,
       canmodify_code,
       dictdescript,
       dictlang,
       parent_code,
       sys_code,
       valid_flag,
       order_no)
    VALUES
      (i_dicttype,
       v_new_dictcode,
       i_dictvalue,
       '',
       i_canmodify_code,
       i_dictdescript,
       i_dictlang,
       i_parent_code,
       i_sys_code,
       i_valid_flag,
       v_new_order_no);
    SELECT MAX(t.dictdescript)
      INTO v_temp
      FROM dams_gen_dict_ref t
     WHERE t.dicttype = i_dicttype
       AND t.sys_code = i_sys_code;
    v_loginfo := '新增字典类型:' || v_temp;
    v_loginfo := v_loginfo || ' 字典项编码:' || v_new_dictcode || ' ';
    v_loginfo := v_loginfo || ' 字典值:' || i_dictvalue;
    SELECT MAX(t.sys_sname)
      INTO v_temp
      FROM dams_sys_conf t
     WHERE t.sys_code = i_sys_code;
    v_loginfo := v_loginfo || ' 所属系统:' || v_temp || ';';
    pckg_dams_util.proc_dams_logger(v_logobj, v_logtype, v_logdate, i_op_user,
                                    v_loginfo);
    COMMIT;
    --pckg_dams_dict_ref.proc_refresh_mview('DAMS_GEN_DICT', 'COMPLETE', o_flag);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_flag := '-1';
      ROLLBACK;
  END proc_add_biz_line;
  
  PROCEDURE proc_dict_edit_order(i_dictlang  IN VARCHAR2, --系统语言
                                 i_sys_code  IN VARCHAR2, --系统code
                                 i_dicttype  IN VARCHAR2, --字典类型
                                 i_dictcodes IN VARCHAR2,
                                 o_ret_code  OUT VARCHAR2, --0  成功
                                 o_msg       OUT VARCHAR2 --
                                 ) IS
    v_param         db_log.info%TYPE := i_dictlang || '|' || i_sys_code || '|' ||
                                        i_dicttype || '|' || i_dictcodes;
    v_proc_name     db_log.proc_name%TYPE := 'pckg_dams_dict_ref.proc_dict_edit_order';
    v_dictcode_list dams_arrytype;
    v_dictcode      ois_gen_dict.dictcode%TYPE;
  BEGIN
    o_ret_code      := '0';
    v_dictcode_list := pckg_dams_util.func_str_to_array(i_dictcodes, ',', '1');

    FOR i IN 1 .. (v_dictcode_list.count)
    LOOP
      v_dictcode := v_dictcode_list(i);
      UPDATE ois_gen_dict d
         SET d.order_no = i
       WHERE d.dicttype = i_dicttype
         AND d.dictcode = v_dictcode
         AND d.dictlang = i_dictlang
         AND d.sys_code = i_sys_code;
    END LOOP;
    UPDATE ois_gen_dict t
       SET t.order_no = NULL
     WHERE t.dicttype = i_dicttype
       AND t.dictlang = i_dictlang
       AND t.sys_code = i_sys_code
       AND t.valid_flag = '0';
    COMMIT;
    --pckg_dams_dict_ref.proc_refresh_mview('DAMS_GEN_DICT', 'COMPLETE', o_ret_code);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_dict_edit_order;
  
END pckg_dams_dict_ref;
//


CREATE OR REPLACE PACKAGE oas_pkg_entegor IS
  FUNCTION if_date_valid(i_date IN entegor_workdate.workdate%TYPE)
    RETURN NUMBER;

  PROCEDURE proc_enlock
  (
    in_data_date IN VARCHAR2,
    out_flag     OUT VARCHAR2
  );

  PROCEDURE proc_unlock
  (
    in_data_date IN VARCHAR2,
    out_flag     OUT VARCHAR2
  );

  FUNCTION func_get_workdate RETURN VARCHAR2;

  PROCEDURE proc_change_workdate
  (
    in_data  IN VARCHAR2,
    out_flag OUT VARCHAR2
  );

  PROCEDURE proc_batlog_writer
  (
    in_workdate  IN VARCHAR2,
    in_prog      IN VARCHAR2,
    in_job       IN VARCHAR2,
    in_batno     IN VARCHAR2,
    in_bgntime   IN VARCHAR2,
    in_endtime   IN VARCHAR2,
    in_runstatus IN VARCHAR2,
    in_notes     IN VARCHAR2
  );

  PROCEDURE proc_baterr_writer
  (
    in_workdate IN VARCHAR2,
    in_prog     IN VARCHAR2,
    in_job      IN VARCHAR2,
    in_errno    IN VARCHAR2,
    in_errtype  IN VARCHAR2,
    in_errlvr   IN VARCHAR2,
    in_errmsg   IN VARCHAR2,
    in_batno    IN VARCHAR2,
    in_notes    IN VARCHAR2
  );
END oas_pkg_entegor;
//

CREATE OR REPLACE PACKAGE BODY oas_pkg_entegor IS
  FUNCTION if_date_valid(i_date IN entegor_workdate.workdate%TYPE)
    RETURN NUMBER IS
    v_date entegor_workdate.workdate%TYPE;
    v_flag NUMBER(1);
  BEGIN
    v_flag := 0;
    SELECT to_char(to_date(i_date, 'yyyymmdd'), 'yyyymmdd') INTO v_date FROM dual;
    IF i_date <> v_date OR i_date IS NULL THEN
      v_flag := 1;
    END IF;
    RETURN(v_flag);
  EXCEPTION
    WHEN OTHERS THEN
      v_flag := 1;
      RETURN(v_flag);
  END if_date_valid;

  PROCEDURE proc_enlock
  (
    in_data_date IN VARCHAR2,
    out_flag     OUT VARCHAR2
  ) IS
    v_flag VARCHAR2(1); --判断标志
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    out_flag := '1';
    SELECT lock_status INTO v_flag FROM entegor_lock;
    IF v_flag <> '0' THEN
      out_flag := '批量加锁失败,目前批量的状态为锁定中!';
      RETURN;
    END IF;
    UPDATE entegor_lock t SET t.lock_status = '1';
    out_flag := '0';
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      out_flag := '批量加锁失败!' || SQLERRM;
  END proc_enlock;

  PROCEDURE proc_unlock
  (
    in_data_date IN VARCHAR2,
    out_flag     OUT VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE entegor_lock t SET t.lock_status = '0';
    out_flag := '0';
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      out_flag := '批量解锁失败!' || SQLERRM;
  END proc_unlock;

  FUNCTION func_get_workdate RETURN VARCHAR2 IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    o_workdate VARCHAR2(8);
  BEGIN
    --目前entegor_workdate采用只记录批量日期的方式，不采用每天新增一条的方式
    SELECT workdate INTO o_workdate FROM entegor_workdate;
    RETURN(o_workdate);
  EXCEPTION
    WHEN OTHERS THEN
      --RETURN to_char(SYSDATE - 1, 'yyyymmdd'); --若表为空则返回系统日期前一日日期
      RETURN to_char(TO_DATE('20200125') - 1, 'yyyymmdd'); --若表为空则返回系统日期前一日日期
  END func_get_workdate;

  PROCEDURE proc_change_workdate
  (
    in_data  IN VARCHAR2,
    out_flag OUT VARCHAR2
  ) IS

    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
	--更新表中的日期加一天
    UPDATE entegor_workdate t
       SET t.workdate = to_char(to_date(t.workdate, 'yyyymmdd') + 1,
                                'yyyymmdd');
    out_flag := '0';
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      out_flag := '批量日期加一天失败!' || SQLERRM;
  END proc_change_workdate;

  PROCEDURE proc_batlog_writer
  (
    in_workdate  IN VARCHAR2,
    in_prog      IN VARCHAR2,
    in_job       IN VARCHAR2,
    in_batno     IN VARCHAR2,
    in_bgntime   IN VARCHAR2,
    in_endtime   IN VARCHAR2,
    in_runstatus IN VARCHAR2,
    in_notes     IN VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO entegor_log
      (id,
       areacode,
       workdate,
       progname,
       jobname,
       readscal,
       updatescal,
       insertscal,
       daltelecacl,
       batchno,
       begintime,
       endtime,
       runstatus,
       notes,
       notes1)
    VALUES
      (seq_entegor_log_id.nextval,
       NULL,
       in_workdate,
       in_prog,
       in_job,
       NULL,
       NULL,
       NULL,
       NULL,
       in_batno,
       in_bgntime,
       NVL(in_endtime, to_char(SYSDATE, 'yyyymmddhh24miss')),
       in_runstatus,
       in_notes,
       NULL);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line('日志插入失败!' || dbms_utility.format_error_stack || ';' || dbms_utility.format_error_backtrace);
      dbms_output.put_line('日志插入失败!' || SQLERRM || ';' || SQLCODE);
  END proc_batlog_writer;

  PROCEDURE proc_baterr_writer
  (
    in_workdate IN VARCHAR2,
    in_prog     IN VARCHAR2,
    in_job      IN VARCHAR2,
    in_errno    IN VARCHAR2,
    in_errtype  IN VARCHAR2,
    in_errlvr   IN VARCHAR2,
    in_errmsg   IN VARCHAR2,
    in_batno    IN VARCHAR2,
    in_notes    IN VARCHAR2
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO entegor_err
      (id,
       areacode,
       workdate,
       systime,
       errno,
       errtype,
       errlvr,
       syserrno,
       errmsg,
       jobname,
       progname,
       batchno,
       tbname1,
       pkey1,
       tbname2,
       pkey2,
       notes,
       notes1)
    VALUES
      (seq_entegor_err_id.nextval,
       NULL,
       in_workdate,
       to_char(systimestamp, 'YYYYMMDDHH24MISS'),
       in_errno,
       in_errtype,
       in_errlvr,
       in_errno,
       in_errmsg,
       in_job,
       in_prog,
       in_batno,
       NULL,
       NULL,
       NULL,
       NULL,
       in_notes,
       NULL);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line('错误日志插入失败!' || dbms_utility.format_error_stack || ';' || dbms_utility.format_error_backtrace);
      dbms_output.put_line('错误日志插入失败!' || SQLERRM || ';' ||SQLCODE);
  END proc_baterr_writer;
END oas_pkg_entegor;
//

CREATE OR REPLACE PACKAGE pckg_dams_role IS
  TYPE ret_list IS REF CURSOR;
  TYPE arrytype IS TABLE OF VARCHAR2(500);
  
  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype;
  
  PROCEDURE proc_role_list(i_branchid IN VARCHAR2, --机构编码
                           --i_branchLevel  IN VARCHAR2, --机构级别
                           i_system    IN VARCHAR2, --??
                           i_language  IN VARCHAR2, --语言编码
                           i_begnum    IN VARCHAR2, --开始取数
                           i_fetchnum  IN VARCHAR2, --获取数
                           i_keyword   IN VARCHAR2, --角色名称关键字
                           i_syscode   IN VARCHAR2, --子系统id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_totalnum  OUT VARCHAR2, --最终获取数
                           o_role_list OUT ret_list --角色列表
                           );
  
  PROCEDURE proc_item_list(i_branchid      IN VARCHAR2, --机构编码
                           i_branchlevel   IN VARCHAR2, --机构级别
                           i_language      IN VARCHAR2, --语言编码
                           o_flag          OUT VARCHAR2, --存储过程返回标志
                           o_ret_item_list OUT ret_list --角色列表
                           );
	
  PROCEDURE proc_role_save(i_name      IN VARCHAR2, --角色名称
                           i_system    IN VARCHAR2, --所属系统
                           i_level     IN VARCHAR2, --所属层级
                           i_branchid  IN VARCHAR2, --创建机构编码
                           i_detail    IN VARCHAR2, --角色描述
                           i_status    IN VARCHAR2, --角色状态
                           i_items     IN VARCHAR2, --操作项
                           i_user      IN VARCHAR2, --操作用户
                           i_language  IN VARCHAR2, --语言编码
                           i_isadmincb IN VARCHAR2, --管理员角色选择，role_type
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_msg       OUT VARCHAR2 --返回信息
                           );
   
   PROCEDURE proc_role_update(i_id        IN VARCHAR2, --角色id
                             i_name      IN VARCHAR2, --角色名称
                             i_system    IN VARCHAR2, --所属系统
                             i_level     IN VARCHAR2, --所属层级
                             i_branchid  IN VARCHAR2, --创建机构编码
                             i_detail    IN VARCHAR2, --角色描述
                             i_status    IN VARCHAR2, --角色状态
                             i_items     IN VARCHAR2, --操作项
                             i_user      IN VARCHAR2, --操作用户
                             i_isadmincb IN VARCHAR2, --管理员角色选择，role_type
                             o_flag      OUT VARCHAR2, --存储过程返回标志
                             o_msg       OUT VARCHAR2 --返回信息
                             );
  
  PROCEDURE proc_role_delete(i_id   IN VARCHAR2, --角色id
                             i_user IN VARCHAR2, --操作用户
                             o_flag OUT VARCHAR2, --存储过程返回标志
                             o_msg  OUT VARCHAR2 --返回信息
                             );
  
  PROCEDURE proc_role_query(i_id      IN VARCHAR2, --角色名称
                            o_flag    OUT VARCHAR2, --存储过程返回标志
                            o_id      OUT VARCHAR2, --id
                            o_name    OUT VARCHAR2, --所属系统
                            o_system  OUT VARCHAR2, --所属系统
                            o_isadmin OUT VARCHAR2, --角色是否管理员
                            o_detail  OUT VARCHAR2, --角色描述
                            o_status  OUT VARCHAR2, --角色状态
                            o_items   OUT VARCHAR2 --操作项
                            );

  PROCEDURE proc_user_role_list(i_syscode        IN VARCHAR2,
                                i_stru           IN VARCHAR2,
                                o_flag           OUT VARCHAR2, --存储过程返回标志
                                o_role_list      OUT ret_list, --角色列表
                                o_user_role_list OUT ret_list --人员角色列表
                                );
								
  PROCEDURE proc_syscode_list(o_flag         OUT VARCHAR2, --存储过程返回标志
                              o_syscode_list OUT ret_list --系统编号列表
                              );
END pckg_dams_role;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_role IS
  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype IS
    i       INTEGER;
    pos     INTEGER;
    k       INTEGER;
    v_count INTEGER := 0;
    objdata arrytype;
  BEGIN
    k       := 1;
    pos     := 1;
    objdata := arrytype();
    i       := instr(tmpstr, param, pos);

    WHILE k <> 0
    LOOP
      k       := instr(tmpstr, param, pos);
      pos     := k + length(param);
      v_count := v_count + 1;
    END LOOP;
    IF i <= 0 THEN
      objdata.extend();
      objdata(1) := tmpstr;
      RETURN objdata;
    END IF;
    pos := 1;
    FOR j IN 1 .. v_count - 1
    LOOP
      objdata.extend();
      i := instr(tmpstr, param, pos);
      objdata(j) := substr(tmpstr, pos, i - 1 - pos + 1);
      pos := i + length(param);
    END LOOP;
    objdata.extend();
    objdata(v_count) := substr(tmpstr, pos, length(tmpstr) - pos + 1);
    RETURN objdata;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
  
  PROCEDURE proc_role_list(i_branchid IN VARCHAR2, --机构编码
                           --i_branchLevel  IN VARCHAR2, --机构级别
                           i_system    IN VARCHAR2, --??
                           i_language  IN VARCHAR2, --语言编码
                           i_begnum    IN VARCHAR2, --开始取数
                           i_fetchnum  IN VARCHAR2, --获取数
                           i_keyword   IN VARCHAR2, --角色名称关键字
                           i_syscode   IN VARCHAR2, --子系统id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_totalnum  OUT VARCHAR2, --最终获取数
                           o_role_list OUT ret_list --角色列表
                           ) IS
    v_where     VARCHAR2(1500) := ' where 1=1 ';
    v_statement VARCHAR2(5000);
  BEGIN
    o_flag := '0';
    IF i_keyword IS NOT NULL THEN
      v_where := v_where || ' and rnls.role_name like ''%' || i_keyword || '%'' ';
    END IF;
    IF i_system IS NOT NULL THEN
      v_where := v_where || ' and role.sys_code  = ''' || i_system || ''' ';
      --else
      --v_where:= v_where||' and role.sys_code in (''SC'',''SA'') ';
    END IF;
    IF i_syscode IS NOT NULL THEN
      v_where := v_where || ' and role.sys_code = ''' || i_syscode || ''' ';
    END IF;
    IF i_language IS NOT NULL THEN
      v_where := v_where || ' and rnls.locale=''' || i_language || ''' ';
    ELSE
      v_where := v_where || ' and rnls.locale=''zh_CN''  ';
    END IF;
    v_statement := 'select role.role_id,
                     rnls.ROLE_NAME,
                     rnls.role_desc,
                     nvl2(role.role_type,''是'',''否'') isAdmin,
                     role.stru_id,
                     bnch.stru_sname,
                     sc.sys_name sysname,
                     role.creator,
                     rownum num
                from dams_role role
                left join dams_role_nls rnls
                  on role.role_id = rnls.role_id
                   left join dams_branch bnch
                  on bnch.stru_id = role.stru_id
                left join dams_sys_conf sc
                  on sc.sys_code = role.sys_code
                  ' || v_where;
	dbms_output.put_line('v_statement= '||v_statement);
    COMMIT;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) from (' || v_statement || ')'
      INTO o_totalnum;

    OPEN o_role_list FOR 'select role_id,
             ROLE_NAME,
             role_desc,
             isAdmin,
             stru_id,
             stru_sname,
             sysname,
             creator
        from (' || v_statement || ')
        where num < (to_number(' || i_begnum || ') + to_number(' || i_fetchnum || '))
        and num >= to_number(' || i_begnum || ')';

  EXCEPTION
    WHEN OTHERS THEN
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_role_list;
  
  PROCEDURE proc_item_list(i_branchid      IN VARCHAR2, --机构编码
                           i_branchlevel   IN VARCHAR2, --机构级别
                           i_language      IN VARCHAR2, --语言编码
                           o_flag          OUT VARCHAR2, --存储过程返回标志
                           o_ret_item_list OUT ret_list --角色列表
                           ) IS
    v_temp VARCHAR2(20);
  BEGIN

    IF i_language IS NULL THEN
      v_temp := 'zh_CN';
    ELSE
      v_temp := i_language;
    END IF;
    o_flag := '0';
    OPEN o_ret_item_list FOR
      SELECT a.menu_id   id,
             a.parent_id pid,
             n.menu_name NAME,
             n.menu_desc remark,
             LEVEL,
             b.cnm       childnum
        FROM dams_menu a,
             (SELECT parent_id, COUNT(1) cnm FROM dams_menu GROUP BY parent_id) b,
             dams_menu_nls n
       WHERE a.menu_id = b.parent_id(+)
         AND a.menu_id = n.menu_id
         AND n.locale = v_temp
       START WITH a.parent_id IS NULL
      CONNECT BY PRIOR a.menu_id = a.parent_id;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_ret_item_list%ISOPEN THEN
        CLOSE o_ret_item_list;
      END IF;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_item_list;
  
  PROCEDURE proc_role_save(i_name      IN VARCHAR2, --角色名称
                           i_system    IN VARCHAR2, --所属系统
                           i_level     IN VARCHAR2, --所属层级
                           i_branchid  IN VARCHAR2, --创建机构编码
                           i_detail    IN VARCHAR2, --角色描述
                           i_status    IN VARCHAR2, --角色状态
                           i_items     IN VARCHAR2, --操作项
                           i_user      IN VARCHAR2, --操作用户
                           i_language  IN VARCHAR2, --语言编码
                           i_isadmincb IN VARCHAR2, --管理员角色选择，role_type
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_msg       OUT VARCHAR2 --返回信息
                           ) IS
    v_temp  VARCHAR2(20);
    v_id    VARCHAR2(20);
    v_items arrytype;
  BEGIN

    IF i_language IS NULL THEN
      v_temp := 'zh_CN';
    ELSE
      v_temp := i_language;
    END IF;

    o_flag := '0';
    o_msg  := '保存成功！';
    pack_log.debug('proc_role_save', to_char(SYSDATE, 'yyyymmdd'),
                   i_name || '|' || i_system || '|' || i_level || '|' ||
                    i_branchid || '|' || i_detail || '|' || i_status || '|' ||
                    i_items || '|' || i_user);

    --获得主键
    SELECT 'RL' || lpad(dams_role_seq.nextval, 8, 0) INTO v_id FROM dual;

    INSERT INTO dams_role
      (role_id,
       role_type,
       sys_code,
       stru_grade,
       stru_id,
       creator,
       create_time,
       lastupdate,
       updator,
       status)
    VALUES
      (v_id,
       i_isadmincb,
       i_system,
       i_level,
       i_branchid,
       i_user,
       to_char(SYSDATE, 'yyyymmdd'),
       to_char(SYSDATE, 'yyyymmdd'),
       i_user,
       i_status);

    INSERT INTO dams_role_nls
      (role_id, role_name, locale, role_desc)
    VALUES
      (v_id, i_name, v_temp, i_detail);

    v_items := func_common_getarray(i_items, '|');
    FOR i IN 1 .. v_items.count
    LOOP
      INSERT INTO dams_acl
        (resource_id, principal_id, principal_type, permission, resource_type)
      VALUES
        (v_items(i), v_id, 'role', 'r', 'menu');
    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      o_msg  := '保存失败！';
      RETURN;
  END proc_role_save;
  
  PROCEDURE proc_role_update(i_id        IN VARCHAR2, --角色id
                             i_name      IN VARCHAR2, --角色名称
                             i_system    IN VARCHAR2, --所属系统
                             i_level     IN VARCHAR2, --所属层级
                             i_branchid  IN VARCHAR2, --创建机构编码
                             i_detail    IN VARCHAR2, --角色描述
                             i_status    IN VARCHAR2, --角色状态
                             i_items     IN VARCHAR2, --操作项
                             i_user      IN VARCHAR2, --操作用户
                             i_isadmincb IN VARCHAR2, --管理员角色选择，role_type
                             o_flag      OUT VARCHAR2, --存储过程返回标志
                             o_msg       OUT VARCHAR2 --返回信息
                             ) IS
    v_items arrytype;
  BEGIN
    o_flag := '0';
    o_msg  := '保存成功！';
    UPDATE dams_role
       SET sys_code   = i_system,
           stru_grade = i_level,
           stru_id    = i_branchid,
           creator    = i_user,
           --kfzx-guoq03         create_time=to_char(sysdate, 'yyyymmdd'),
           lastupdate = to_char(SYSDATE, 'yyyymmdd'),
           updator    = i_user,
           status     = i_status,
           role_type  = i_isadmincb
     WHERE role_id = i_id;

    UPDATE dams_role_nls
       SET role_name = i_name, role_desc = i_detail
     WHERE role_id = i_id;
    DELETE dams_acl WHERE principal_id = i_id;
    v_items := func_common_getarray(i_items, '|');
    FOR i IN 1 .. v_items.count
    LOOP
      INSERT INTO dams_acl
        (resource_id, principal_id, principal_type, permission, resource_type)
      VALUES
        (v_items(i), i_id, 'role', 'r', 'menu');
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      o_msg  := '保存失败！';
      RETURN;
  END proc_role_update;
  
  PROCEDURE proc_role_delete(i_id   IN VARCHAR2, --角色id
                             i_user IN VARCHAR2, --操作用户
                             o_flag OUT VARCHAR2, --存储过程返回标志
                             o_msg  OUT VARCHAR2 --返回信息
                             ) IS
    v_sysadmin VARCHAR2(20);

  BEGIN

    o_flag := '0';
    o_msg  := '删除成功！';

    SELECT t.role_type INTO v_sysadmin FROM dams_role t WHERE t.role_id = i_id;

    IF v_sysadmin = 'SYSADMIN' THEN
      o_flag := '-1';
      o_msg  := '删除失败，超级管理员不能删除！';
    ELSE
      DELETE dams_role WHERE role_id = i_id;

      DELETE dams_acl WHERE principal_id = i_id;

      DELETE dams_role_nls WHERE role_id = i_id;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      o_msg  := '删除失败！';
      RETURN;
  END proc_role_delete;
  
  PROCEDURE proc_role_query(i_id      IN VARCHAR2, --角色名称
                            o_flag    OUT VARCHAR2, --存储过程返回标志
                            o_id      OUT VARCHAR2, --id
                            o_name    OUT VARCHAR2, --所属系统
                            o_system  OUT VARCHAR2, --所属系统
                            o_isadmin OUT VARCHAR2, --角色是否管理员
                            o_detail  OUT VARCHAR2, --角色描述
                            o_status  OUT VARCHAR2, --角色状态
                            o_items   OUT VARCHAR2 --操作项

                            ) IS

  BEGIN
    o_flag := 0;

    pack_log.debug('proc_role_query', to_char(SYSDATE, 'yyyymmdd'), i_id);
    COMMIT;
    SELECT role.role_id,
           nls.role_name,
           role.sys_code,
           nvl2(role.role_type, 'ADMIN', '') isadmin,
           nls.role_desc,
           role.status,
           '1'
      INTO o_id, o_name, o_system, o_isadmin, o_detail, o_status, o_items
      FROM dams_role role, dams_role_nls nls
     WHERE role.role_id = nls.role_id
       AND role.role_id = i_id
       and nls.locale = 'zh_CN';

    SELECT listagg(acl.resource_id) within GROUP(ORDER BY 1)
      INTO o_items
      FROM dams_acl acl
     WHERE acl.principal_id = i_id;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_role_query;
  
  PROCEDURE proc_user_role_list(i_syscode        IN VARCHAR2,
                                i_stru           IN VARCHAR2,
                                o_flag           OUT VARCHAR2, --存储过程返回标志
                                o_role_list      OUT ret_list, --角色列表
                                o_user_role_list OUT ret_list --人员角色列表
                                ) IS
    v_step      db_log.step_no%TYPE;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_role.proc_user_role_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg);
    o_flag := '0';
    v_step := '1';
    OPEN o_user_role_list FOR
      SELECT rel.role_id, usr.name || '  [' || bnch.stru_sname || ']'
        FROM dams_user_role_rel rel
        LEFT JOIN dams_user usr
          ON (rel.ssic_id = usr.ssic_id)
        LEFT JOIN dams_branch bnch
          ON (rel.stru_id = bnch.stru_id)
       WHERE rel.sys_code = i_syscode
         AND EXISTS
       (SELECT 1
                FROM dams_branch_relation rela, dams_branch_sys_rel sysr
               WHERE rela.bnch_id = rel.stru_id
                 AND sysr.sys_code = i_syscode
                 AND sysr.stru_id = i_stru
                 AND rela.up_bnch_id = sysr.stru_id
                 AND sysr.start_use = '1' --启用
              )
         AND NOT EXISTS
       (SELECT 1
                FROM dams_branch_relation rela, dams_branch_sys_rel sysr
               WHERE rela.bnch_id = rel.stru_id
                 AND sysr.sys_code = i_syscode
                 AND sysr.stru_id = i_stru
                 AND rela.up_bnch_id = sysr.stru_id
                 AND sysr.start_use = '0' --停用
              )
       ORDER BY rel.role_id, bnch.stru_sname;
    v_step := '2';
    OPEN o_role_list FOR
      SELECT DISTINCT rol.role_id, nls.role_name || '【' || rol.role_id || '】'
        FROM dams_role rol
        LEFT JOIN dams_role_nls nls
          ON (rol.role_id = nls.role_id)
       WHERE rol.sys_code = i_syscode
       ORDER BY rol.role_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      IF o_user_role_list%ISOPEN THEN
        CLOSE o_user_role_list;
      END IF;
      pack_log.error(v_proc_name, v_step, SQLCODE);

      RETURN;
  END proc_user_role_list;
  
  PROCEDURE proc_syscode_list(o_flag         OUT VARCHAR2, --存储过程返回标志
                              o_syscode_list OUT ret_list --子系统列表

                              ) IS
    v_step      db_log.step_no%TYPE;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_role.proc_syscode_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg);
    o_flag := '0';
    v_step := '1';
    OPEN o_syscode_list FOR
      SELECT conf.sys_code, conf.sys_name FROM dams_sys_conf conf;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      IF o_syscode_list%ISOPEN THEN
        CLOSE o_syscode_list;
      END IF;
      pack_log.error(v_proc_name, v_step, SQLCODE);

      RETURN;
  END proc_syscode_list;
  
END pckg_dams_role;
//


CREATE OR REPLACE PACKAGE pckg_dams_mail IS
  FUNCTION func_get_mailaddr(i_ssicid IN VARCHAR2) RETURN VARCHAR2;
  
  FUNCTION func_inner_subject(i_subject     IN CLOB,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB;
  
  FUNCTION func_inner_content(i_content     IN CLOB,
                              i_username    IN VARCHAR2,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_syscode     IN VARCHAR2,
                              i_task_id     IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB;
  
  FUNCTION func_get_mail_seq RETURN VARCHAR2;
  
  PROCEDURE proc_get_mail_template(i_tpl_id      IN VARCHAR2,
                                   o_subject_tpl OUT VARCHAR2,
                                   o_content_tpl OUT VARCHAR2);
  
  PROCEDURE proc_get_unsend_mail(i_hostip   IN VARCHAR2, --主机IP
                                 o_ret_code OUT VARCHAR2, --返回值
                                 o_cur      OUT SYS_REFCURSOR);
  
  PROCEDURE proc_insert_todo_mail(i_cfgid        IN VARCHAR2, --配置项编号
                                  i_pdid         IN VARCHAR2, --流程id
                                  i_language     IN VARCHAR2, --语言
                                  i_task_id      IN VARCHAR2, --任务ID
                                  i_next_task_id IN VARCHAR2, --下一任务ID
                                  i_business_id  IN VARCHAR2, --业务主键
                                  i_sys_code     IN VARCHAR2, --子模块编号
                                  i_activity_id  IN VARCHAR2, --活动定义id
                                  i_mail_tpl_id  IN VARCHAR2, --通用模板编号
                                  i_mail_to      IN VARCHAR2, --主送人
                                  i_mail_cc      IN VARCHAR2, --抄送人
                                  i_mail_bcc     IN VARCHAR2, --密送人
                                  i_title        IN VARCHAR2, --标题
                                  i_dest_name    IN VARCHAR2,
                                  o_ret_code     OUT VARCHAR2 --返回值
                                  );
  
  PROCEDURE proc_insert_custom_mail(i_mail_to    IN VARCHAR2, --主送人
                                    i_mail_cc    IN VARCHAR2, --抄送人
                                    i_mail_bcc   IN VARCHAR2, --密送人
                                    i_title      IN VARCHAR2, --标题
                                    i_content    IN VARCHAR2, --内容
                                    i_translated IN VARCHAR2, -- 是否需要地址转换
                                    o_ret_code   OUT VARCHAR2 --返回值
                                    );
  
  PROCEDURE proc_insert_custom_mail_new(i_tpl_id     IN VARCHAR2, --模板ID
                                        i_tpl_param  IN VARCHAR2, --模板参数
                                        i_mail_to    IN VARCHAR2, --发送人
                                        i_mail_cc    IN VARCHAR2, --抄送人
                                        i_mail_bcc   IN VARCHAR2, --密送人
                                        i_mail_type  IN VARCHAR2, --发送类型
                                        i_translated IN VARCHAR2, -- 是否需要地址转换
                                        o_ret_code   OUT VARCHAR2 --返回值
                                        );
  
  PROCEDURE proc_update_mail_status(i_mail_id  IN VARCHAR2, --邮件id
                                    i_status   IN VARCHAR2, --状态
                                    o_ret_code OUT VARCHAR2 --返回值
                                    );
  
  PROCEDURE proc_insert_todo_mail_vm(i_mail_to         IN VARCHAR2,
                                     i_pdid            IN VARCHAR2,
                                     i_cfgid           IN VARCHAR2,
                                     i_token           IN VARCHAR2,
                                     i_title           IN VARCHAR2,
                                     i_mail_tpl_id     IN VARCHAR2,
                                     i_dest_name       IN VARCHAR2,
                                     i_transition_name IN VARCHAR2,
                                     i_business_id     IN VARCHAR2,
                                     o_ret_code        OUT VARCHAR2);
  
  PROCEDURE proc_insert_st_mail(i_mail_to     IN VARCHAR2,
                                i_token       IN VARCHAR2,
                                i_title       IN VARCHAR2,
                                i_mail_tpl_id IN VARCHAR2,
                                i_business_id IN VARCHAR2,
                                o_ret_code    OUT VARCHAR2);
  
  FUNCTION func_gen_rndstr(i_len IN NUMBER) RETURN VARCHAR2;
  
  PROCEDURE proc_insert_custom_mail_token(i_tpl_id     IN VARCHAR2, --模板ID
                                         i_tpl_param  IN VARCHAR2, --模板参数
                                         i_mail_to    IN VARCHAR2, --发送人
                                         i_mail_cc    IN VARCHAR2, --抄送人
                                         i_mail_bcc   IN VARCHAR2, --密送人
                                         i_mail_type  IN VARCHAR2, --发送类型
                                         i_translated IN VARCHAR2, -- 是否需要地址转换
                                         o_ret_code   OUT VARCHAR2 --返回值
                                         );
  
  PROCEDURE proc_insert_inner_todo_mail(i_ssic_id     IN VARCHAR2,
                                        i_mail_to     IN VARCHAR2,
                                        i_business_id IN VARCHAR2,
                                        i_task_id     IN VARCHAR2,
                                        i_sys_code    IN VARCHAR2,
                                        i_title       in varchar2,
                                        o_ret_code    OUT VARCHAR2,
                                        o_msg         OUT VARCHAR2);
  
END pckg_dams_mail;
//


CREATE OR REPLACE PACKAGE BODY pckg_dams_mail IS
  FUNCTION func_get_mailaddr(i_ssicid IN VARCHAR2) RETURN VARCHAR2 IS
    v_retval CLOB := '';
    v_tmpval VARCHAR2(4000) := '';
    v_sql    VARCHAR2(4000) := '';
  BEGIN
    v_sql := 'SELECT LISTAGG(t.Notes_Id,'','') within group(order by t.notes_id) ' || ' FROM dams_user t ' || ' WHERE t.ssic_id IN (SELECT column_value FROM TABLE(func_split(:1,'','')))';
    EXECUTE IMMEDIATE v_sql INTO v_retval USING i_ssicid;
    RETURN v_retval;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_mailaddr;
  
  FUNCTION func_inner_subject(i_subject     IN CLOB,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB IS
    v_subject CLOB;
  BEGIN
    SELECT REPLACE(REPLACE(REPLACE(i_subject, '$subject', i_title), '$businessId', i_business_id), '$destName', i_dest_name) INTO v_subject FROM dual;
    RETURN v_subject;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_inner_subject;
  
  FUNCTION func_inner_content(i_content     IN CLOB,
                              i_username    IN VARCHAR2,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_syscode     IN VARCHAR2,
                              i_task_id     IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB IS
    v_content      CLOB;
    v_finalopinion dams_wf_hist_task.outgoing_name%TYPE;
  BEGIN
    SELECT MAX(m.outgoing_name)
      INTO v_finalopinion
      FROM (SELECT decode(t.outgoing_name, 'default_transition', '审批通过',
                          t.outgoing_name) outgoing_name,
                   row_number() over(PARTITION BY t.business_id ORDER BY t.create_time DESC, t.task_id DESC) rn
              FROM dams_wf_hist_task t
             WHERE t.business_id = i_business_id) m
     WHERE m.rn = '1';
    SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(i_content, '$userName', i_username), '$subject', i_title), '$businessId', i_business_id), '$taskId', i_task_id), '$destName', i_dest_name), '$finalopinion', v_finalopinion) INTO v_content FROM dual;
    RETURN v_content;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_inner_content;
  
  FUNCTION func_get_mail_seq RETURN VARCHAR2 IS
    v_seq VARCHAR2(12) := '';
  BEGIN
    SELECT lpad(dams_mail_seq.nextval, 12, '0') INTO v_seq FROM dual;
    RETURN v_seq;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_mail_seq;
  
  PROCEDURE proc_get_mail_template(i_tpl_id      IN VARCHAR2,
                                   o_subject_tpl OUT VARCHAR2,
                                   o_content_tpl OUT VARCHAR2) IS
  BEGIN
    SELECT t.tpl_subject, t.tpl_content
      INTO o_subject_tpl, o_content_tpl
      FROM dams_mail_template t
     WHERE t.tpl_id = i_tpl_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_subject_tpl := '';
      o_content_tpl := '';
  END proc_get_mail_template;
  
  PROCEDURE proc_get_unsend_mail(i_hostip   IN VARCHAR2, --主机IP
                                 o_ret_code OUT VARCHAR2, --返回值
                                 o_cur      OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_hostip;
    v_proc_name  db_log.proc_name%TYPE := 'pckg_dams_mail.proc_get_unsend_mail';
    v_step       db_log.step_no%TYPE;
    v_systime    TIMESTAMP;
    unsend_mails dams_arrytype;
  BEGIN
    o_ret_code := 0;
    BEGIN
      EXECUTE IMMEDIATE 'SELECT t.dictvalue FROM ois_gen_dict t WHERE t.dicttype = ''MAIL_LOCK'' AND t.dictcode = ''1'' and t.sys_code = ''DAMS'' FOR UPDATE';
    EXCEPTION
      WHEN OTHERS THEN
        pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
        o_ret_code := -1;
        RETURN;
    END;
    SELECT systimestamp INTO v_systime FROM dual;
    UPDATE dams_mail t
       SET t.send_flag = '1', t.load_time = v_systime, t.host_ip = i_hostip
     WHERE t.send_flag = '0'
       AND rownum <= 100
    RETURNING t.mail_id BULK COLLECT INTO unsend_mails;
    OPEN o_cur FOR
      SELECT m.mail_id,
             m.mail_to,
             m.mail_cc,
             m.mail_bcc,
             m.subject,
             m.content,
             m.template_id,
             m.template_data
        FROM dams_mail m, TABLE(unsend_mails) b
       WHERE b.column_value = m.mail_id;
    COMMIT;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_unsend_mail;
  
  PROCEDURE proc_insert_todo_mail(i_cfgid        IN VARCHAR2, --配置项编号
                                  i_pdid         IN VARCHAR2, --流程id
                                  i_language     IN VARCHAR2, --语言
                                  i_task_id      IN VARCHAR2, --任务ID
                                  i_next_task_id IN VARCHAR2, --下一任务ID
                                  i_business_id  IN VARCHAR2, --业务主键
                                  i_sys_code     IN VARCHAR2, --子模块编号
                                  i_activity_id  IN VARCHAR2, --活动定义id
                                  i_mail_tpl_id  IN VARCHAR2, --通用模板编号
                                  i_mail_to      IN VARCHAR2, --主送人
                                  i_mail_cc      IN VARCHAR2, --抄送人
                                  i_mail_bcc     IN VARCHAR2, --密送人
                                  i_title        IN VARCHAR2, --标题
                                  i_dest_name    IN VARCHAR2, --节点名称
                                  o_ret_code     OUT VARCHAR2 --返回值
                                  ) IS
    v_param       db_log.info%TYPE := i_cfgid || '|' || i_pdid || '|' ||
                                      i_language || '|' || i_task_id || '|' ||
                                      i_next_task_id || '|' || i_business_id || '|' ||
                                      i_sys_code || '|' || i_activity_id || '|' ||
                                      i_mail_tpl_id || '|' || i_sys_code || '|' ||
                                      i_mail_tpl_id || '|' || i_mail_to || '|' ||
                                      i_mail_cc || '|' || i_mail_bcc || '|' ||
                                      i_title || '|' || i_dest_name;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_todo_mail';
    v_mail_seq    VARCHAR2(12);
    v_mailto_addr VARCHAR2(300);
    v_mailto_name VARCHAR2(90);
    v_taskid      VARCHAR2(2000);
    v_taskid2     VARCHAR2(19);
    v_subject     CLOB; --主题模板
    v_content     CLOB; --内容模板
    arr_mail_to   dbms_sql.varchar2_table;
    arr_business_id dbms_sql.varchar2_table;
    arr_title       dbms_sql.varchar2_table;
    arr_taskid      dbms_sql.varchar2_table;
    v_mail_tpl_id   VARCHAR2(12);
    v_ssic_id       VARCHAR2(10);
  BEGIN
    --邮件暂时不提供 密送和抄送功能！
    o_ret_code := 0;
    arr_mail_to := func_dams_getarrayfromstr(i_mail_to, ',');
    arr_business_id := func_dams_getarrayfromstr(i_business_id, '$|$');
    arr_title       := func_dams_getarrayfromstr(i_title, '$|$');
    v_taskid        := nvl(i_next_task_id, i_task_id);
    arr_taskid      := func_dams_getarrayfromstr(v_taskid, ',');
	dbms_output.put_line('arr_business_id='||arr_business_id);
    IF i_next_task_id IS NULL THEN
      --next_task_Id为空，则为办结状态，向申请人通知
      v_mail_tpl_id := nvl(i_mail_tpl_id, '2'); --标准流程结束通知模板
      BEGIN 
        SELECT m.subject_tpl, m.content_tpl
          INTO v_subject, v_content
          FROM dams_wf_mail_cfg m
         WHERE m.cfgid = i_cfgid
           AND m.activity_id = i_activity_id
           AND m.language = i_language;
      EXCEPTION
        WHEN no_data_found THEN
          SELECT nvl(t.tpl_subject, t1.tpl_subject),
                 nvl(t.tpl_content, t1.tpl_content)
            INTO v_subject, v_content
            FROM dams_mail_template t, dams_mail_template t1
           WHERE t.language(+) = i_language
             AND t.tpl_id(+) = t1.tpl_id
             AND t1.language = 'zh_CN'
             AND t1.tpl_id = v_mail_tpl_id; --指定通知模板
      END;
      FOR i IN 1 .. arr_business_id.count
      LOOP
        --不指定主送人 ，则主送人为申请人
        IF i_mail_to IS NULL THEN
          SELECT MAX(u.notes_id), MAX(u.name), MAX(u.ssic_id)
            INTO v_mailto_addr, v_mailto_name, v_ssic_id
            FROM dams_wf_hist_process p, dams_user u
           WHERE p.business_id = arr_business_id(i)
             AND p.creator_id = u.ssic_id
             AND p.start_time =
                 (SELECT MIN(p2.start_time)
                    FROM dams_wf_hist_process p2
                   WHERE p2.business_id = arr_business_id(i));  
        ELSE
          v_mailto_addr := func_get_mailaddr(i_mail_to);
        END IF;
        v_mail_seq := func_get_mail_seq();
        INSERT INTO dams_mail(mail_id, mail_to, mail_cc, mail_bcc, subject, content)
        VALUES
          (v_mail_seq, --id
           v_mailto_addr, --主送
           '', --抄送
           '', --密送
           func_inner_subject(v_subject, arr_title(i), arr_business_id(i), i_dest_name), --主题
           func_inner_content(v_content, v_mailto_name, arr_title(i), arr_business_id(i), i_sys_code, v_taskid, i_dest_name)); --内容
        INSERT INTO dams_wf_task_mail(mail_id, task_id, business_id, sys_code, mail_to_ssic)
        VALUES(v_mail_seq, v_taskid, arr_business_id(i), i_sys_code, v_ssic_id);
      END LOOP;
    ELSE
      --暂时用单发模式，群发模式测试失败
      FOR i IN 1 .. arr_mail_to.count
      LOOP
        --主送人ssicId不为空，则为审批人通知
        IF arr_mail_to(i) IS NOT NULL THEN
          v_mail_tpl_id := nvl(i_mail_tpl_id, '1'); --标准审批通知模板 
          --主题 , 内容, 链接
          BEGIN
            --没有传入activity_id时的临时处理方式
            SELECT m.subject_tpl, m.content_tpl
              INTO v_subject, v_content
              FROM dams_wf_mail_cfg m
             WHERE m.cfgid = i_cfgid
               AND m.activity_id =
                   (SELECT c.id
                      FROM dams_wf_task t, dams_wf_activity_conf c
                     WHERE t.task_id = i_task_id
                       AND t.task_name = c.activity_name
                       AND c.cfgid = m.cfgid)
               AND m.language = i_language;
          EXCEPTION
            WHEN no_data_found THEN
              SELECT t.tpl_subject, t.tpl_content
                INTO v_subject, v_content
                FROM dams_mail_template t
               WHERE t.language = i_language
                 AND t.tpl_id = v_mail_tpl_id; --指定通知模板
          END;
          --主送人地址
          SELECT MAX(u.notes_id), MAX(u.name)
            INTO v_mailto_addr, v_mailto_name
            FROM dams_user u
           WHERE u.ssic_id = arr_mail_to(i); 
          --可以存在多条业务申请 mod by xiesy 20130328
          FOR j IN 1 .. arr_business_id.count
          LOOP    
            --判断是否有多条taskid
            IF arr_taskid IS NOT NULL AND arr_taskid.count > 1 THEN
              --判断是否多条businessId
              IF arr_business_id.count > 1 THEN
                --多个businessId,则是一个单子需要拆流程的,task序号和businessId序号对应
                v_taskid2 := arr_taskid(j);
              ELSE
                --单个businessId,多个taskId，则fork分支情况，暂不处理
                NULL;
              END IF;
            ELSE
              --只有一个taskId,直接赋值
              v_taskid2 := arr_taskid(1);
            END IF;
            v_mail_seq := func_get_mail_seq(); 
            INSERT INTO dams_mail (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
            VALUES
              (v_mail_seq, --id
               v_mailto_addr, --主送
               '', --抄送
               '', --密送
               func_inner_subject(v_subject, arr_title(j), arr_business_id(j), i_dest_name), --主题
               func_inner_content(v_content, v_mailto_name, arr_title(j), arr_business_id(j), i_sys_code, v_taskid2, i_dest_name)); --内容
            INSERT INTO dams_wf_task_mail(mail_id, task_id, business_id, sys_code, mail_to_ssic)  VALUES (v_mail_seq, v_taskid2, arr_business_id(j), i_sys_code, arr_mail_to(i));
          END LOOP;
        END IF;
      END LOOP;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || SQLERRM || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_todo_mail;
  
  PROCEDURE proc_insert_custom_mail(i_mail_to    IN VARCHAR2, --主送人
                                    i_mail_cc    IN VARCHAR2, --抄送人
                                    i_mail_bcc   IN VARCHAR2, --密送人
                                    i_title      IN VARCHAR2, --标题
                                    i_content    IN VARCHAR2, --内容
                                    i_translated IN VARCHAR2, -- 是否需要地址转换
                                    o_ret_code   OUT VARCHAR2 --返回值
                                    ) IS
    v_param        db_log.info%TYPE := substrb(i_mail_to || '|' || i_mail_cc || '|' ||
                                               i_mail_bcc || '|' || i_title || '|' ||
                                               i_content, 1, 3900);
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail';
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(300);
    v_mailcc_addr  VARCHAR2(300);
    v_mailbcc_addr VARCHAR2(300);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    dbms_sql.varchar2_table;
    arr_mail_cc    dbms_sql.varchar2_table;
    arr_mail_bcc   dbms_sql.varchar2_table;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);
  BEGIN
    o_ret_code := 0;
    IF i_translated <> '1' THEN
      v_mailto_addr  := func_get_mailaddr(i_mail_to);
      v_mailcc_addr  := func_get_mailaddr(i_mail_cc);
      v_mailbcc_addr := func_get_mailaddr(i_mail_bcc);
    ELSE
      v_mailto_addr  := i_mail_to;
      v_mailcc_addr  := i_mail_cc;
      v_mailbcc_addr := i_mail_bcc;
    END IF;
    v_mail_seq := func_get_mail_seq();
    --单条发送解析用
    arr_mail_to := func_dams_getarrayfromstr(v_mailto_addr, ',');
    --arr_mail_cc  := func_dams_getarrayfromstr(v_mailcc_addr, ',');
    --arr_mail_bcc := func_dams_getarrayfromstr(v_mailbcc_addr, ',');
    FOR i IN 1 .. arr_mail_to.count
    LOOP
      v_mail_seq := func_get_mail_seq();
      INSERT INTO dams_mail
        (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
      VALUES
        (v_mail_seq,
         arr_mail_to(i),
         v_mailcc_addr,
         v_mailbcc_addr,
         i_title,
         i_content);
    END LOOP;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail;
  
  PROCEDURE proc_insert_custom_mail_new(i_tpl_id     IN VARCHAR2, --模板ID
                                        i_tpl_param  IN VARCHAR2, --模板参数
                                        i_mail_to    IN VARCHAR2, --发送人
                                        i_mail_cc    IN VARCHAR2, --抄送人
                                        i_mail_bcc   IN VARCHAR2, --密送人
                                        i_mail_type  IN VARCHAR2, --发送类型
                                        i_translated IN VARCHAR2, -- 是否需要地址转换
                                        o_ret_code   OUT VARCHAR2 --返回值
                                        ) IS
    v_param        db_log.info%TYPE := i_tpl_id || '|' || i_mail_to || '|' ||
                                       i_mail_type || '|' || i_translated;
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail_new';
    v_step         db_log.step_no%TYPE;
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(4000);
    v_mailcc_addr  VARCHAR2(4000);
    v_mailbcc_addr VARCHAR2(4000);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    dams_arrytype;
    arr_mail_cc    dams_arrytype;
    arr_mail_bcc   dams_arrytype;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);
  BEGIN
    o_ret_code := 0;
    IF nvl(i_translated, '0') <> '1' THEN
      v_mailto_addr  := func_get_mailaddr(i_mail_to);
      v_mailcc_addr  := func_get_mailaddr(i_mail_cc);
      v_mailbcc_addr := func_get_mailaddr(i_mail_bcc);
    ELSE
      v_mailto_addr  := i_mail_to;
      v_mailcc_addr  := i_mail_cc;
      v_mailbcc_addr := i_mail_bcc;
    END IF;
    --单条发送解析用
    arr_mail_to := pckg_dams_util.func_str_to_array(v_mailto_addr, ',');
    FOR i IN 1 .. arr_mail_to.count
    LOOP
      v_mail_seq := func_get_mail_seq();
      INSERT INTO dams_mail
        (mail_id,
         mail_type,
         mail_to,
         mail_cc,
         mail_bcc,
         template_id,
         template_data)
      VALUES
        (v_mail_seq,
         i_mail_type,
         arr_mail_to(i),
         v_mailcc_addr,
         v_mailbcc_addr,
         i_tpl_id,
         i_tpl_param);
    END LOOP;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail_new;
  
  PROCEDURE proc_update_mail_status(i_mail_id  IN VARCHAR2, --邮件id
                                    i_status   IN VARCHAR2, --状态
                                    o_ret_code OUT VARCHAR2 --返回值
                                    ) IS
    v_param     db_log.info%TYPE := i_mail_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_mail.proc_update_mail_status';
    v_step      db_log.step_no%TYPE;
    v_idx       INTEGER;
  BEGIN
    o_ret_code := 0;
    IF i_status = '0' THEN
      UPDATE dams_mail t
         SET t.send_flag = '8', --发送成功
             t.send_time = systimestamp
       WHERE t.mail_id = i_mail_id;
    ELSE
      UPDATE dams_mail t
         SET t.send_flag = '9', --发送失败
             t.send_time = systimestamp
       WHERE t.mail_id = i_mail_id;
    END IF;
    COMMIT;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_update_mail_status;
  
  PROCEDURE proc_insert_todo_mail_vm(i_mail_to         IN VARCHAR2,
                                     i_pdid            IN VARCHAR2,
                                     i_cfgid           IN VARCHAR2,
                                     i_token           IN VARCHAR2,
                                     i_title           IN VARCHAR2,
                                     i_mail_tpl_id     IN VARCHAR2,
                                     i_dest_name       IN VARCHAR2,
                                     i_transition_name IN VARCHAR2,
                                     i_business_id     IN VARCHAR2,
                                     o_ret_code        OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := i_mail_to || '|' || i_pdid || '|' ||
                                      i_cfgid || '|' || i_token || '|' ||
                                      i_title || '|' || i_dest_name || '|' ||
                                      i_mail_tpl_id || '|' || i_transition_name || '|' ||
                                      i_business_id;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_todo_mail_vm';
    v_dest_name   VARCHAR2(255);
    v_mail_seq    VARCHAR(12);
    v_mail_tpl_id VARCHAR(12);
    v_mailto_addr VARCHAR(100);
    v_mailtype    VARCHAR(12);
    v_applyer     dams_user.name%TYPE;
    v_data        CLOB;
  BEGIN
    o_ret_code := 0;
    SELECT MAX(u.name)
      INTO v_applyer
      FROM dams_user u, dams_wf_process p
     WHERE u.ssic_id = p.creator_id
       AND p.business_id = i_business_id
       AND p.cfg_id = nvl(i_cfgid, '1')
       AND p.process_id = i_pdid;
    --pckg_dams_jbpm.proc_jbpm_activity_alias(i_pdid, nvl(i_cfgid, '1'), i_transition_name, i_dest_name, o_ret_code, v_dest_name);
    v_mailtype := '1';
    v_mail_seq := func_get_mail_seq();
    v_mail_tpl_id := nvl(i_mail_tpl_id, '1');
    SELECT MAX(t.notes_id)
      INTO v_mailto_addr
      FROM dams_user t
     WHERE t.ssic_id = i_mail_to;
    IF v_mailto_addr IS NOT NULL THEN
      v_data := 'destName=' || v_dest_name || '&applyer=' || v_applyer ||
                '&subject=' || i_title || '&businessId=' || i_business_id ||
                '&token=' || i_token;
      INSERT INTO dams_mail
        (mail_id, mail_type, mail_to, template_id, template_data)
      VALUES
        (v_mail_seq, v_mailtype, v_mailto_addr, v_mail_tpl_id, v_data);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_todo_mail_vm;
  
  PROCEDURE proc_insert_st_mail(i_mail_to     IN VARCHAR2,
                                i_token       IN VARCHAR2,
                                i_title       IN VARCHAR2,
                                i_mail_tpl_id IN VARCHAR2,
                                i_business_id IN VARCHAR2,
                                o_ret_code    OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := i_mail_to || '|' || i_token || '|' ||
                                      i_title || '|' || i_mail_tpl_id || '|' ||
                                      i_business_id;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_st_mail';
    v_mail_seq    VARCHAR(12);
    v_mail_tpl_id VARCHAR(12);
    v_mailto_addr VARCHAR(100);
    v_title       VARCHAR(1000);
    v_mailtype    VARCHAR(12);
    v_data        CLOB;
  BEGIN
    o_ret_code := 0;
    v_mailtype := '1';
    v_mail_seq := func_get_mail_seq();
    v_mail_tpl_id := nvl(i_mail_tpl_id, '1');
    SELECT MAX(t.notes_id)
      INTO v_mailto_addr
      FROM dams_user t
     WHERE t.ssic_id = i_mail_to;
    SELECT t.title
      INTO v_title
      FROM dams_wf_hist_process t
     WHERE t.business_id = i_business_id;
    v_data := '&subject=' || v_title || '&businessId=' || i_business_id || '&token=' || i_token;
    INSERT INTO dams_mail (mail_id, mail_type, mail_to, template_id, template_data)
    VALUES (v_mail_seq, v_mailtype, v_mailto_addr, v_mail_tpl_id, v_data);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_st_mail;
  
  FUNCTION func_gen_rndstr(i_len IN NUMBER) RETURN VARCHAR2 IS
    v_ret        VARCHAR2(1000);
    v_candidates VARCHAR2(62) := '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    v_chr        VARCHAR2(1);
  BEGIN
    FOR i IN 1 .. i_len
    LOOP
      BEGIN
        SELECT substr(v_candidates, floor(dbms_random.value(0, 62)), 1)
          INTO v_chr
          FROM dual;
        v_ret := v_ret || v_chr;
      END;
    END LOOP;
    RETURN v_ret;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_gen_rndstr;
  
  PROCEDURE proc_insert_custom_mail_token(i_tpl_id     IN VARCHAR2, --模板ID
                                          i_tpl_param  IN VARCHAR2, --模板参数
                                          i_mail_to    IN VARCHAR2, --发送人
                                          i_mail_cc    IN VARCHAR2, --抄送人
                                          i_mail_bcc   IN VARCHAR2, --密送人
                                          i_mail_type  IN VARCHAR2, --发送类型
                                          i_translated IN VARCHAR2, -- 是否需要地址转换
                                          o_ret_code   OUT VARCHAR2 --返回值
                                          ) IS
    v_param        db_log.info%TYPE := i_tpl_id || '|' || i_tpl_param || '|' ||
                                       i_mail_to || '|' || i_mail_cc || '|' ||
                                       i_mail_bcc || '|' || i_mail_type || '|' ||
                                       i_translated;
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail_token';
    v_step         db_log.step_no%TYPE;
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(4000);
    v_mailcc_addr  VARCHAR2(4000);
    v_mailbcc_addr VARCHAR2(4000);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    pckg_dams_role.arrytype;
    arr_mail_cc    dams_arrytype;
    arr_mail_bcc   dams_arrytype;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);
    v_token     VARCHAR2(40) := ''; --token字段
    v_url_param VARCHAR2(4000) := '';
    arr_param   pckg_dams_role.arrytype;
    c_done_task_tpl_id CONSTANT VARCHAR2(20) := '34,131';
    v_is_done_task VARCHAR2(1);
  BEGIN
    o_ret_code := 0;
    -- 是否method要使用done_task
    SELECT COUNT(1)
      INTO v_is_done_task
      FROM TABLE(func_split2(c_done_task_tpl_id))
     WHERE column_value = i_tpl_id;
    --arr_param := pckg_dams_role.func_common_getarray(i_tpl_param, '&');
    FOR i IN 1 .. arr_param.count
    LOOP
      IF instr(arr_param(i), 'businessId') > 0 THEN
        v_url_param := v_url_param || '&' || REPLACE(arr_param(i), 'businessId', 'business_id');
      END IF;
      IF instr(arr_param(i), 'taskId') > 0 THEN
        v_url_param := v_url_param || '&' || REPLACE(arr_param(i), 'taskId', 'jbpm_task_id');
      END IF;
    END LOOP;
    --单条发送解析用
    --arr_mail_to := pckg_dams_role.func_common_getarray(i_mail_to, ',');
    FOR i IN 1 .. arr_mail_to.count
    LOOP
      v_mail_seq := func_get_mail_seq();
      v_token    := func_gen_rndstr(40);
      IF nvl(i_translated, '0') <> '1' THEN
        INSERT INTO dams_mail_token
          (token_id,
           ssic_id,
           method,
           url_param,
           create_time,
           dealed_time,
           expire_time)
        VALUES
          (v_token,
           arr_mail_to(i),
           decode(v_is_done_task, '1', 'opendonetask', 'opentodotask'),
           v_url_param,
           to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
           NULL,
           to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));
        INSERT INTO dams_mail
          (mail_id,
           mail_type,
           mail_to,
           mail_cc,
           mail_bcc,
           template_id,
           template_data)
          SELECT v_mail_seq,
                 i_mail_type,
                 u.notes_id,
                 v_mailcc_addr,
                 v_mailbcc_addr,
                 i_tpl_id,
                 i_tpl_param || '&token=' || v_token
            FROM dams_user u
           WHERE u.ssic_id = arr_mail_to(i);
      ELSE
        BEGIN
          SELECT u.ssic_id
            INTO v_ssic_id
            FROM dams_user u
           WHERE u.notes_id = arr_mail_to(i)
             AND rownum = 1;
        EXCEPTION
          WHEN OTHERS THEN
            v_ssic_id := '';
        END;
        INSERT INTO dams_mail_token
          (token_id,
           ssic_id,
           method,
           url_param,
           create_time,
           dealed_time,
           expire_time)
        VALUES
          (v_token,
           v_ssic_id,
           decode(v_is_done_task, '1', 'opendonetask', 'opentodotask'),
           v_url_param,
           to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
           NULL,
           to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));
        INSERT INTO dams_mail
          (mail_id,
           mail_type,
           mail_to,
           mail_cc,
           mail_bcc,
           template_id,
           template_data)
        VALUES
          (v_mail_seq,
           i_mail_type,
           arr_mail_to(i),
           v_mailcc_addr,
           v_mailbcc_addr,
           i_tpl_id,
           i_tpl_param || '&token=' || v_token);
      END IF;
    END LOOP;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail_token;
  
  PROCEDURE proc_insert_inner_todo_mail(i_ssic_id     IN VARCHAR2,
                                        i_mail_to     IN VARCHAR2,
                                        i_business_id IN VARCHAR2,
                                        i_task_id     IN VARCHAR2,
                                        i_sys_code    IN VARCHAR2,
                                        i_title       IN VARCHAR2,
                                        o_ret_code    OUT VARCHAR2,
                                        o_msg         OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_mail_to || '|' ||
                                    i_business_id || '|' || i_task_id || '|' ||
                                    i_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_inner_todo_mail';
    arr_mail_to pckg_dams_role.arrytype;
    v_mail_seq  VARCHAR2(12);
    v_token     VARCHAR2(40) := ''; --token字段
    v_url_param VARCHAR2(4000) := '';
    c_tpl_id NUMBER(5) := 35;
    c_method VARCHAR2(20) := 'openinnertodo';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := '0';
    v_url_param := '&business_id=' || i_business_id || '&jbpm_task_id=' || i_task_id;  
    --单条发送解析用
    --arr_mail_to := pckg_dams_role.func_common_getarray(i_mail_to, ',');
    FOR i IN 1 .. arr_mail_to.count
    LOOP
      v_mail_seq := func_get_mail_seq();
      v_token    := func_gen_rndstr(40);
      INSERT INTO dams_mail_token
        (token_id,
         ssic_id,
         method,
         url_param,
         create_time,
         dealed_time,
         expire_time)
      VALUES
        (v_token,
         arr_mail_to(i),
         c_method,
         v_url_param,
         to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
         NULL,
         to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));
      INSERT INTO dams_mail
        (mail_id,
         mail_type,
         mail_to,
         mail_cc,
         mail_bcc,
         template_id,
         template_data)
        SELECT v_mail_seq,
               '1',
               u.notes_id,
               '',
               '',
               c_tpl_id,
               '&token=' || v_token || '&subject=' || i_title
          FROM dams_user u
         WHERE u.ssic_id = arr_mail_to(i);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_insert_inner_todo_mail;
  
END pckg_dams_mail;
//


CREATE OR REPLACE PACKAGE pckg_ab_rpt_yuezhi IS
  PROCEDURE proc_add_yuezhi(i_businessid IN VARCHAR2,
                            i_userid     IN CLOB,
                            i_cretperson IN VARCHAR2,
                            i_taskid     IN NUMBER,
                            o_flag       OUT VARCHAR2, --存储过程成功失败标志
                            o_errmsg     OUT VARCHAR2,
                            o_mail_user  OUT VARCHAR2);
   
   PROCEDURE proc_add_yuezhi_fxpc(i_businessid IN VARCHAR2,
                                 i_userid     IN CLOB,
                                 i_cretperson IN VARCHAR2,
                                 i_taskid     IN NUMBER,
                                 o_flag       OUT VARCHAR2, --存储过程成功失败标志
                                 o_errmsg     OUT VARCHAR2,
                                 o_mail_user  OUT VARCHAR2);
   
   PROCEDURE proc_auto_add_yuezhi(i_businessid IN VARCHAR2, --申请单号
                                 i_nextstep   IN VARCHAR2,
                                 i_creator    IN VARCHAR2,
                                 i_taskid     IN NUMBER,
                                 o_flag       OUT VARCHAR2, --存储过程成功失败标志
                                 o_errmsg     OUT VARCHAR2,
                                 o_mail_user  OUT VARCHAR2);
   
   PROCEDURE proc_auto_comment_yuezhi(i_businessid  IN VARCHAR2, --申请单号
                                     i_commentflag IN VARCHAR2,
                                     i_taskid      IN VARCHAR2,
                                     o_flag        OUT VARCHAR2, --存储过程成功失败标志
                                     o_errmsg      OUT VARCHAR2,
                                     o_mail_user   OUT VARCHAR2);
   
   PROCEDURE proc_yuezhi_qry(i_ssic_id   IN VARCHAR2, --用户号
                            i_beg_num   IN VARCHAR2, --起始页数
                            i_fetch_num IN VARCHAR2, --每页获取数据量
                            i_read_type IN VARCHAR2, --阅读状态
                            o_retcode   OUT VARCHAR2, --返回值
                            o_total_num OUT VARCHAR2, --记录总数
                            o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                            );
   
   PROCEDURE proc_yuezhi_state_update(i_business_id IN VARCHAR2, --稿纸ID
                                     i_ssic_id     IN VARCHAR2, --进来人ID
                                     i_task_id     IN VARCHAR2, --流程ID
                                     o_retcode     OUT VARCHAR2 --返回值
                                     );
   
END pckg_ab_rpt_yuezhi;
//


CREATE OR REPLACE PACKAGE BODY pckg_ab_rpt_yuezhi IS
  PROCEDURE proc_add_yuezhi(i_businessid IN VARCHAR2,
                            i_userid     IN CLOB,
                            i_cretperson IN VARCHAR2,
                            i_taskid     IN NUMBER,
                            o_flag       OUT VARCHAR2, --存储过程成功失败标志
                            o_errmsg     OUT VARCHAR2,
                            o_mail_user  OUT VARCHAR2) IS
  
    v_param     db_log.info%TYPE := i_businessid || '|' || substr(i_userid, 0, 3000) || '|' || i_cretperson || '|' || i_taskid;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_rpt_yuezhi.proc_add_yuezhi';
    v_count         NUMBER;
    v_sql           VARCHAR2(2024);
    v_str           VARCHAR2(32000);
    v_length        INTEGER := 0;
    v_begnum        INTEGER := 1;
    v_postion       INTEGER := 1;
    v_cnt           INTEGER := 0;
    v_userarray     pckg_dams_role.arrytype;
    v_readuserarray pckg_dams_role.arrytype;
    v_title         dams_wf_process.title%TYPE;
    v_tpl_param     VARCHAR2(4000) := '';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_flag := 0;
    IF i_userid IS NULL THEN
      RETURN;
    END IF;
    v_length := dbms_lob.getlength(i_userid);
    IF v_length <= 8000 THEN
      dbms_lob.read(i_userid, v_length, 1, v_str);
      --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
      FOR i IN 1 .. v_userarray.count
      LOOP
        v_count := 1;
        v_sql   := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and  t.user_id=:i_userId';  
        --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), '|');  
        EXECUTE IMMEDIATE v_sql
          INTO v_count
          USING i_businessid, v_readuserarray(1);
        --如果已插过阅知表，则不用再插
        IF v_count = 0 THEN
          o_mail_user := o_mail_user || v_readuserarray(1) || ',';
          INSERT INTO dams_read_empower
            (key_id,
             bussiness_id,
             user_id,
             create_time,
             create_user,
             state,
             task_id)
          VALUES
            (seq_read_empower.nextval,
             i_businessid,
             v_readuserarray(1),
             SYSDATE,
             i_cretperson,
             '0',
             i_taskid);
        END IF;
      END LOOP;
    ELSE
      LOOP
        EXIT WHEN v_length - v_postion < 8000;
        v_postion := dbms_lob.instr(i_userid, ',', v_postion, 100);
        v_str     := dbms_lob.substr(i_userid, v_postion - v_begnum, v_begnum);
        --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
        FOR i IN 1 .. v_userarray.count
        LOOP
          v_count := 1;       
          --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), '|');      
          v_sql := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and t.user_id=:i_userId';
          EXECUTE IMMEDIATE v_sql
            INTO v_count
            USING i_businessid, v_readuserarray(1);       
          --如果已插过阅知表，则不用再插
          IF v_count = 0 THEN
            o_mail_user := o_mail_user || v_readuserarray(1) || ',';
            INSERT INTO dams_read_empower
              (key_id,
               bussiness_id,
               user_id,
               create_time,
               create_user,
               state,
               task_id)
            VALUES
              (seq_read_empower.nextval,
               i_businessid,
               v_readuserarray(1),
               SYSDATE,
               i_cretperson,
               '0',
               i_taskid);
          END IF;
        END LOOP;     
        v_begnum := v_postion;
        IF v_length - v_postion < 8000 THEN
          v_str := dbms_lob.substr(i_userid, v_length - v_begnum + 1, v_begnum);    
          --v_userarray := pckg_dams_role.func_common_getarray(v_str, '|');
          FOR i IN 1 .. v_userarray.count
          LOOP
            v_count := 1;
            v_sql   := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and t.user_id=:i_userId';
            --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), ',');
            EXECUTE IMMEDIATE v_sql
              INTO v_count
              USING i_businessid, v_readuserarray(1); 
            --如果已插过阅知表，则不用再插
            IF v_count = 0 THEN
              o_mail_user := o_mail_user || v_readuserarray(1) || ',';
              INSERT INTO dams_read_empower
                (key_id,
                 bussiness_id,
                 user_id,
                 create_time,
                 create_user,
                 state,
                 task_id)
              VALUES
                (seq_read_empower.nextval,
                 i_businessid,
                 v_readuserarray(1),
                 SYSDATE,
                 i_cretperson,
                 '0',
                 i_taskid);
            END IF;
          END LOOP;  
        END IF;
        v_cnt := v_cnt + 1;
      END LOOP;
    END IF;
    --发阅知邮件
    o_mail_user := substr(o_mail_user, 1, length(o_mail_user) - 1);
    IF i_businessid LIKE 'FXPC%' THEN
      SELECT to_char(substr(info.description, 1, 1000))
        INTO v_title
        FROM ab_rsk_chk_info info
       WHERE info.rsk_chk_id = i_businessid;
    ELSE
      SELECT MAX(p.title)
        INTO v_title
        FROM viw_dams_wf_all_process p
       WHERE p.business_id = i_businessid;
    END IF;
    v_tpl_param := 'businessId=' || i_businessid || '&title=' || v_title || '&taskId=' || i_taskid;
    pckg_dams_mail.proc_insert_custom_mail_token('34', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := -1;
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      o_errmsg := v_proc_name || ':' || SQLERRM(SQLCODE);
      ROLLBACK;
  END proc_add_yuezhi;
  
  PROCEDURE proc_add_yuezhi_fxpc(i_businessid IN VARCHAR2,
                                 i_userid     IN CLOB,
                                 i_cretperson IN VARCHAR2,
                                 i_taskid     IN NUMBER,
                                 o_flag       OUT VARCHAR2, --存储过程成功失败标志
                                 o_errmsg     OUT VARCHAR2,
                                 o_mail_user  OUT VARCHAR2) IS
  
    v_param            db_log.info%TYPE := i_businessid || '|' || substr(i_userid, 0, 3000) || '|' || i_cretperson || '|' || i_taskid;
    v_proc_name        db_log.proc_name%TYPE := 'pckg_ab_rpt_yuezhi.proc_add_yuezhi_fxpc';
    v_business_id      ab_rsk_chk_info.rsk_chk_id%TYPE;
    v_business_id_list dams_arrytype;
    v_userid           dams_user.ssic_id%TYPE;
    v_userid_list      dams_arrytype;
    v_isread_flag      NUMBER; --是否已经插过阅知表标志位
    v_remind_msg       VARCHAR2(4000);
    v_title            VARCHAR2(4000) := '';
    v_tpl_param        VARCHAR2(4000) := '';
    v_tokenid          VARCHAR2(40);
    v_mail_flag        dams_user.notes_id%TYPE;
  
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_flag := 0;
    IF i_userid IS NULL THEN
      RETURN;
    END IF;
    v_business_id_list := pckg_dams_util.func_str_to_array(i_businessid, ',', '1');
    v_userid_list      := pckg_dams_util.func_str_to_array(i_userid, ',', '1');
    FOR i IN 1 .. (v_business_id_list.count)
    LOOP
      v_business_id := v_business_id_list(i);
      FOR j IN 1 .. (v_userid_list.count)
      LOOP
        v_userid      := v_userid_list(j);
        o_mail_user   := o_mail_user || v_userid || ',';
        v_isread_flag := 1;
        SELECT COUNT(1)
          INTO v_isread_flag
          FROM dams_read_empower emp
         WHERE emp.bussiness_id = v_business_id
           AND emp.user_id = v_userid;
        IF v_isread_flag = 0 THEN
          INSERT INTO dams_read_empower
            (key_id,
             bussiness_id,
             user_id,
             create_time,
             create_user,
             state,
             task_id)
          VALUES
            (seq_read_empower.nextval,
             v_business_id,
             v_userid,
             SYSDATE,
             i_cretperson,
             '0',
             '');
        END IF;
        --发邮件提醒
        SELECT du.notes_id
          INTO v_mail_flag
          FROM dams_user du
         WHERE du.ssic_id = v_userid;
        IF v_mail_flag IS NOT NULL THEN
          v_tokenid := pckg_dams_mail.func_gen_rndstr(20);
          INSERT INTO dams_mail_token
            (token_id,
             state,
             ssic_id,
             method,
             url_param,
             create_time,
             expire_time)
          VALUES
            (v_tokenid,
             0,
             v_userid,
             'rskchk',
             '&business_id=' || v_business_id,
             to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
             to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));  
          v_title      := '风险隐患事件台账,单号：' || v_business_id || '请您阅知';
          v_remind_msg := '以下信息请您阅知：';
          v_tpl_param := 'remind_msg=' || v_remind_msg || '&title=' || v_title || '&rsk_chk_id=' || v_business_id || '&opFlag=' || 'detail' || '&stru_id=' || '' || '&token=' || v_tokenid || '&fromtype=' || 'yz';
          pckg_dams_mail.proc_insert_custom_mail_new('166', v_tpl_param, v_userid, '', '', '5', '0', o_flag);
        END IF;
      END LOOP;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := -1;
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      o_errmsg := v_proc_name || ':' || SQLERRM(SQLCODE);
      ROLLBACK;
  END proc_add_yuezhi_fxpc;
  
  PROCEDURE proc_auto_add_yuezhi(i_businessid IN VARCHAR2, --申请单号
                                 i_nextstep   IN VARCHAR2,
                                 i_creator    IN VARCHAR2,
                                 i_taskid     IN NUMBER,
                                 o_flag       OUT VARCHAR2, --存储过程成功失败标志
                                 o_errmsg     OUT VARCHAR2,
                                 o_mail_user  OUT VARCHAR2) IS
    v_param              db_log.info%TYPE := i_businessid || '|' || i_nextstep || '|' || i_taskid;
    v_initialbussinessid VARCHAR2(20);
    v_initialtaskid      VARCHAR2(19);
    v_yuezhitype         VARCHAR2(1); --1下发；２下级机构反馈后
    v_processtype        VARCHAR2(1);
    v_str                VARCHAR2(32000);
    v_userid             CLOB;
    v_length             INTEGER := 0;
    v_userarray          pckg_dams_role.arrytype;
    v_readuserarray      pckg_dams_role.arrytype;
    v_title              dams_wf_process.title%TYPE;
    v_parameter          dams_branch.stru_sname%TYPE;
    v_tpl_param          VARCHAR2(4000) := '';
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_rpt_yuezhi.proc_auto_add_yuezhi';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_flag        := 0;
    v_processtype := 1; --默认主办单位
    --提示单、通知送下级机构后，自动发送阅知并邮件给分管办公室主任、发起单位“经办人”“复核人”
    IF i_nextstep NOT IN
       ('通知发送', '送主办和知会单位', '同意并送主办和知会单位', '同意并反馈上级机构', '同意并返回办公室', '同意并返回上级机构',
        '同意/知晓并反馈办公室', '同意并反馈办公室') THEN
      RETURN;
    END IF;
    IF i_nextstep IN ('通知发送', '送主办和知会单位', '同意并送主办和知会单位') THEN
      --通知单
      v_yuezhitype := 1;
      IF i_businessid LIKE 'SYTZ%' THEN
        SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
          INTO v_userid
          FROM (
                --办公室主任
                SELECT cha.ssic_id
                  FROM dams_charge_info cha, dams_notice_report_app app
                 WHERE app.business_id = i_businessid
                   AND app.app_stru = cha.stru_id
                   AND cha.model_id = 'M0001'
                   AND cha.sys_code = 'AB'
                UNION
                --发起单位经办人
                SELECT rr.ssic_id
                  FROM dams_notice_report_app app, dams_user_role_rel rr
                 WHERE app.business_id = i_businessid
                   AND rr.role_id = 'AB007'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND app.app_stru = rr.stru_id
                UNION
                --发起单位复核人
                SELECT rr.ssic_id
                  FROM dams_notice_report_app app, dams_user_role_rel rr
                 WHERE app.business_id = i_businessid
                   AND rr.role_id = 'AB002'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND app.app_stru = rr.stru_id) tmp;     
      ELSIF i_businessid LIKE 'SYFX%' THEN
        --提示单
        SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
          INTO v_userid
          FROM (
                --办公室主任
                SELECT cha.ssic_id
                  FROM dams_charge_info cha, ab_rpt_rsk_app_wf wf
                 WHERE wf.business_id = i_businessid
                   AND wf.app_stru = cha.stru_id
                   AND cha.model_id = 'M0001'
                   AND cha.sys_code = 'AB'
                UNION
                --发起单位经办人
                SELECT rr.ssic_id
                  FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                 WHERE wf.business_id = i_businessid
                   AND rr.role_id = 'AB000'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND wf.app_stru = rr.stru_id
                UNION
                --发起单位复核人
                SELECT rr.ssic_id
                  FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                 WHERE wf.business_id = i_businessid
                   AND rr.role_id = 'AB002'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND wf.app_stru = rr.stru_id) tmp;    
      ELSE
        RETURN;
      END IF;
    ELSIF i_nextstep IN ('同意并反馈上级机构', '同意并返回办公室', '同意并返回上级机构', '同意/知晓并反馈办公室', '同意并反馈办公室') THEN
      v_yuezhitype := 2;
      SELECT t.source_business_id
        INTO v_initialbussinessid
        FROM dams_wf_sub_process_rel t
       WHERE t.business_id = i_businessid;
      SELECT tmp.task_id
        INTO v_initialtaskid
        FROM (SELECT t.task_id,
                     row_number() over(ORDER BY t.create_time DESC) rn
                FROM viw_dams_wf_all_task t
               WHERE t.business_id = v_initialbussinessid) tmp
       WHERE tmp.rn = '1';
      --下面不好,换成上面的那个
      --通知单
      IF i_businessid LIKE 'SYTZ%' THEN
        --分管主任及“经办人”“复核人”、发起单位增设阅知人员
        SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
          INTO v_userid
          FROM (
                --办公室主任
                SELECT cha.ssic_id
                  FROM dams_charge_info cha, dams_notice_report_app app
                 WHERE app.business_id = v_initialbussinessid
                   AND app.app_stru = cha.stru_id
                   AND cha.model_id = 'M0001'
                   AND cha.sys_code = 'AB'
                UNION
                --发起单位经办人
                SELECT rr.ssic_id
                  FROM dams_notice_report_app app, dams_user_role_rel rr
                 WHERE app.business_id = v_initialbussinessid
                   AND rr.role_id = 'AB007'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND app.app_stru = rr.stru_id
                UNION
                --发起单位复核人
                SELECT rr.ssic_id
                  FROM dams_notice_report_app app, dams_user_role_rel rr
                 WHERE app.business_id = v_initialbussinessid
                   AND rr.role_id = 'AB002'
                   AND rr.auth_state IN ('0', '1', '2')
                   AND app.app_stru = rr.stru_id
                UNION
                --发起单位增设阅知人员
                SELECT emp.user_id
                  FROM dams_read_empower emp
                 WHERE emp.bussiness_id = v_initialbussinessid) tmp;
      ELSIF i_businessid LIKE 'SYFX%' THEN
        SELECT t.type
          INTO v_processtype
          FROM ab_rpt_rsk_app_wf_bnch_rel t
         WHERE t.business_id = v_initialbussinessid
           AND t.stru_id = (SELECT wf.stru_id
                              FROM ab_rpt_rsk_app_wf wf
                             WHERE wf.business_id = i_businessid);
        --主办单位反馈：分管主任及“经办人”“复核人”、发起单位增设阅知人员、知会部门的“经办人”或“联系人”
        IF v_processtype = 1 THEN
          SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
            INTO v_userid
            FROM (
                  --办公室主任
                  SELECT cha.ssic_id
                    FROM dams_charge_info cha, ab_rpt_rsk_app_wf wf
                   WHERE wf.business_id = v_initialbussinessid
                     AND wf.app_stru = cha.stru_id
                     AND cha.model_id = 'M0001'
                     AND cha.sys_code = 'AB'
                  UNION
                  --发起单位经办人
                  SELECT rr.ssic_id
                    FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                   WHERE wf.business_id = v_initialbussinessid
                     AND rr.role_id = 'AB000'
                     AND rr.auth_state IN ('0', '1', '2')
                     AND wf.app_stru = rr.stru_id
                  UNION
                  --发起单位复核人
                  SELECT rr.ssic_id
                    FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                   WHERE wf.business_id = v_initialbussinessid
                     AND rr.role_id = 'AB002'
                     AND rr.auth_state IN ('0', '1', '2')
                     AND wf.app_stru = rr.stru_id
                  UNION
                  --发起单位增设阅知人员
                  SELECT emp.user_id
                    FROM dams_read_empower emp
                   WHERE emp.bussiness_id = v_initialbussinessid
                  --知会部门的“经办人”或“联系人”
                  UNION
                  SELECT t2.ssic_id
                    FROM ab_rpt_rsk_app_wf_bnch_rel t1, dams_user_role_rel t2
                   WHERE t1.business_id = v_initialbussinessid
                     AND t1.type = '2'
                     AND t1.stru_id = t2.stru_id
                     AND t2.role_id IN ('AB000', 'AB001')
                     AND t2.auth_state IN ('0', '1', '2')) tmp;
          --知会单位反馈：分管主任及“经办人”“复核人”、发起单位增设阅知人员
        ELSE
          SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
            INTO v_userid
            FROM (
                  --办公室主任
                  SELECT cha.ssic_id
                    FROM dams_charge_info cha, ab_rpt_rsk_app_wf wf
                   WHERE wf.business_id = v_initialbussinessid
                     AND wf.app_stru = cha.stru_id
                     AND cha.model_id = 'M0001'
                     AND cha.sys_code = 'AB'
                  UNION
                  --发起单位经办人
                  SELECT rr.ssic_id
                    FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                   WHERE wf.business_id = v_initialbussinessid
                     AND rr.role_id = 'AB000'
                     AND rr.auth_state IN ('0', '1', '2')
                     AND wf.app_stru = rr.stru_id
                  UNION
                  --发起单位复核人
                  SELECT rr.ssic_id
                    FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
                   WHERE wf.business_id = v_initialbussinessid
                     AND rr.role_id = 'AB002'
                     AND rr.auth_state IN ('0', '1', '2')
                     AND wf.app_stru = rr.stru_id
                  UNION
                  --发起单位增设阅知人员
                  SELECT emp.user_id
                    FROM dams_read_empower emp
                   WHERE emp.bussiness_id = v_initialbussinessid) tmp;
        END IF;
      ELSE
        RETURN;
      END IF;
    ELSE
      RETURN;
    END IF;
    v_length := dbms_lob.getlength(v_userid);
    dbms_lob.read(v_userid, v_length, 1, v_str);
    --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
    FOR i IN 1 .. v_userarray.count
    LOOP
      --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), '|');
      o_mail_user     := o_mail_user || v_readuserarray(1) || ',';
      IF v_yuezhitype = 1 THEN
        INSERT INTO dams_read_empower
          (key_id,
           bussiness_id,
           user_id,
           create_time,
           create_user,
           state,
           task_id)
        VALUES
          (seq_read_empower.nextval,
           i_businessid,
           v_readuserarray(1),
           SYSDATE,
           i_creator,
           '0',
           i_taskid);
      ELSE
        INSERT INTO dams_read_empower
          (key_id,
           bussiness_id,
           user_id,
           create_time,
           create_user,
           state,
           task_id)
        VALUES
          (seq_read_empower.nextval,
           v_initialbussinessid,
           v_readuserarray(1),
           SYSDATE,
           i_creator,
           '0',
           v_initialtaskid);
      END IF;
    END LOOP;
    --发阅知邮件
    o_mail_user := substr(o_mail_user, 1, length(o_mail_user) - 1);
    SELECT MAX(p.title)
      INTO v_title
      FROM viw_dams_wf_all_process p
     WHERE p.business_id = i_businessid;
    IF v_yuezhitype = 1 THEN
      --下发
      v_tpl_param := 'businessId=' || i_businessid || '&title=' || v_title || '&taskId=' || i_taskid;
      IF i_businessid LIKE 'SYTZ%' THEN
        pckg_dams_mail.proc_insert_custom_mail_token('37', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
      ELSIF i_businessid LIKE 'SYFX%' THEN
        pckg_dams_mail.proc_insert_custom_mail_token('36', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
      END IF;
    ELSIF v_yuezhitype = 2 THEN
      --反馈
      IF i_businessid LIKE 'SYTZ%' THEN
        SELECT (SELECT b.stru_sname
                  FROM dams_branch b
                 WHERE b.stru_id = t.stru_id)
          INTO v_parameter
          FROM dams_noti_app_feedback t
         WHERE t.sub_app_id = i_businessid;  
        v_tpl_param := 'businessId=' || v_initialbussinessid || '&title=' || v_title || '&taskId=' || v_initialtaskid || '&name=' || v_parameter;
        pckg_dams_mail.proc_insert_custom_mail_token('39', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
      ELSIF i_businessid LIKE 'SYFX%' THEN
        SELECT db.stru_sname
          INTO v_parameter
          FROM dams_branch db, ab_rpt_rsk_app_wf wf
         WHERE wf.business_id = i_businessid
           AND wf.stru_id = db.stru_id;
        v_tpl_param := 'businessId=' || v_initialbussinessid || '&title=' || v_title || '&taskId=' || v_initialtaskid || '&name=' || v_parameter;
        pckg_dams_mail.proc_insert_custom_mail_token('38', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
      END IF;
    ELSE
      RETURN;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := -1;
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      o_errmsg := v_proc_name || ':' || SQLERRM(SQLCODE);
      ROLLBACK;
  END proc_auto_add_yuezhi;
  
  PROCEDURE proc_auto_comment_yuezhi(i_businessid  IN VARCHAR2, --申请单号
                                     i_commentflag IN VARCHAR2,
                                     i_taskid      IN VARCHAR2,
                                     o_flag        OUT VARCHAR2, --存储过程成功失败标志
                                     o_errmsg      OUT VARCHAR2,
                                     o_mail_user   OUT VARCHAR2) IS
    v_param         db_log.info%TYPE := i_businessid || '|' || i_commentflag || '|' || i_taskid;
    v_str           VARCHAR2(32000);
    v_userid        CLOB;
    v_length        INTEGER := 0;
    v_userarray     pckg_dams_role.arrytype;
    v_readuserarray pckg_dams_role.arrytype;
    v_title         dams_wf_process.title%TYPE;
    v_tpl_param     VARCHAR2(4000) := '';
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_rpt_yuezhi.proc_auto_comment_yuezhi';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_flag := 0;
    --提示单
    SELECT listagg(tmp.ssic_id, ',') within GROUP(ORDER BY tmp.ssic_id)
      INTO v_userid
      FROM (
            --发起单位经办人
            SELECT rr.ssic_id
              FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
             WHERE wf.business_id = i_businessid
               AND rr.role_id = 'AB000'
               AND rr.auth_state IN ('0', '1', '2')
               AND wf.app_stru = rr.stru_id
            UNION
            --发起单位复核人
            SELECT rr.ssic_id
              FROM ab_rpt_rsk_app_wf wf, dams_user_role_rel rr
             WHERE wf.business_id = i_businessid
               AND rr.role_id = 'AB002'
               AND rr.auth_state IN ('0', '1', '2')
               AND wf.app_stru = rr.stru_id) tmp;
    v_length := dbms_lob.getlength(v_userid);
    dbms_lob.read(v_userid, v_length, 1, v_str);
    --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
    FOR i IN 1 .. v_userarray.count
    LOOP
      --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), '|');
      o_mail_user     := o_mail_user || v_readuserarray(1) || ','; 
    END LOOP;
    --发阅知邮件
    o_mail_user := substr(o_mail_user, 1, length(o_mail_user) - 1);
    SELECT MAX(p.title)
      INTO v_title
      FROM viw_dams_wf_all_process p
     WHERE p.business_id = i_businessid;
    v_tpl_param := 'businessId=' || i_businessid || '&title=' || v_title || '&taskId=' || i_taskid;
    pckg_dams_mail.proc_insert_custom_mail_token('34', v_tpl_param, o_mail_user, '', '', '2', '', o_flag);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := -1;
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      o_errmsg := v_proc_name || ':' || SQLERRM(SQLCODE);
      ROLLBACK;
  END proc_auto_comment_yuezhi;
  
  PROCEDURE proc_yuezhi_qry(i_ssic_id   IN VARCHAR2, --用户号
                            i_beg_num   IN VARCHAR2, --起始页数
                            i_fetch_num IN VARCHAR2, --每页获取数据量
                            i_read_type IN VARCHAR2, --阅读状态
                            o_retcode   OUT VARCHAR2, --返回值
                            o_total_num OUT VARCHAR2, --记录总数
                            o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                            ) IS
  
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_beg_num || '|' || i_fetch_num || '|' || i_read_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_rpt_yuezhi.proc_yuezhi_qry';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --开始包含FXPC单号
    SELECT COUNT(1)
      INTO o_total_num
      FROM (SELECT e.bussiness_id,
                   p.title,
                   to_char(e.create_time, 'yyyymmdd'),
                   e.state,
                   e.task_id
              FROM dams_read_empower e, viw_dams_wf_all_process p
             WHERE e.bussiness_id = p.business_id
               AND p.sys_code = 'AB'
               AND e.user_id = i_ssic_id
               AND e.state = nvl(i_read_type, e.state)
            UNION ALL
            SELECT e.bussiness_id,
                   to_char(substr(info.description, 1, 1000)) title,
                   to_char(e.create_time, 'yyyymmdd'),
                   e.state,
                   e.task_id
              FROM dams_read_empower e, ab_rsk_chk_info info
             WHERE e.bussiness_id LIKE 'FXPC%'
               AND e.bussiness_id = info.rsk_chk_id
               AND info.status = '1'
               AND e.user_id = i_ssic_id
               AND e.state = nvl(i_read_type, e.state));
    OPEN o_list_cur FOR
      SELECT business_id, title, cret_date, cret_person, task_id
        FROM (SELECT tmp.business_id,
                     tmp.title,
                     tmp.cret_date,
                     tmp.create_date,
                     tmp.cret_person,
                     tmp.task_id,
                     row_number() over(ORDER BY tmp.create_date DESC) rn
                FROM (SELECT business_id,
                             title,
                             decode(state, '1',
                                    '<span class=work_read>' || cret_date ||
                                     '</span>',
                                    '<span class=work_unread>' || cret_date ||
                                     '</span>') cret_date,
                             cret_date create_date,
                             b.name cret_person,
                             task_id
                        FROM (SELECT e.bussiness_id business_id,
                                     p.title,
                                     e.state,
                                     e.task_id,
                                     to_char(e.create_time, 'yyyymmdd') cret_date,
                                     e.create_user
                                FROM dams_read_empower       e,
                                     viw_dams_wf_all_process p
                               WHERE e.bussiness_id = p.business_id
                                 AND p.sys_code = 'AB'
                                 AND e.user_id = i_ssic_id
                                 AND e.state = nvl(i_read_type, e.state)) a,
                             dams_user b
                       WHERE a.create_user = b.ssic_id
                      UNION ALL
                      SELECT business_id,
                             title,
                             decode(state, '1',
                                    '<span class=work_read>' || cret_date ||
                                     '</span>',
                                    '<span class=work_unread>' || cret_date ||
                                     '</span>') cret_date,
                             cret_date create_date,
                             b.name cret_person,
                             task_id
                        FROM (SELECT e.bussiness_id business_id,
                                     to_char(substr(info.description, 1, 1000)) title,
                                     e.state,
                                     e.task_id,
                                     to_char(e.create_time, 'yyyymmdd') cret_date,
                                     e.create_user
                                FROM dams_read_empower e, ab_rsk_chk_info info
                               WHERE e.bussiness_id LIKE 'FXPC%'
                                 AND info.rsk_chk_id = e.bussiness_id
                                 AND info.status = '1'
                                 AND e.user_id = i_ssic_id
                                 AND e.state = nvl(i_read_type, e.state)) a,
                             dams_user b
                       WHERE a.create_user = b.ssic_id) tmp)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num)
       ORDER BY rn;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      o_retcode := '-1';
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      RETURN;
  END proc_yuezhi_qry;
  
  PROCEDURE proc_yuezhi_state_update(i_business_id IN VARCHAR2, --稿纸ID
                                     i_ssic_id     IN VARCHAR2, --进来人ID
                                     i_task_id     IN VARCHAR2, --流程ID
                                     o_retcode     OUT VARCHAR2 --返回值
                                     ) IS
    v_param     db_log.info%TYPE := i_business_id || '|' || i_ssic_id || '|' || i_task_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_MYWORK.proc_yuezhi_state_update';
  BEGIN
    o_retcode := '0';
    IF i_task_id = 'null' OR i_task_id IS NULL THEN
      UPDATE dams_read_empower t
         SET t.state = '1'
       WHERE t.bussiness_id = i_business_id
         AND t.user_id = i_ssic_id;
    ELSE
      UPDATE dams_read_empower t
         SET t.state = '1'
       WHERE t.bussiness_id = i_business_id
         AND t.task_id = i_task_id
         AND t.user_id = i_ssic_id;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
  END proc_yuezhi_state_update;
  
END pckg_ab_rpt_yuezhi;
//


CREATE OR REPLACE PACKAGE pckg_dams_todo_count_send IS
  FUNCTION func_get_total_count(i_ssic_id IN VARCHAR2 --用户ID
                                ) RETURN VARCHAR2;
  
  PROCEDURE proc_insert_users(i_users    IN VARCHAR2, --待办数量变化用户，以,分割
                              o_ret_code OUT VARCHAR2 --返回值
                              );
  
  PROCEDURE proc_get_assignees(i_task_id  IN VARCHAR2, --当前任务ID
                               o_ret_code OUT VARCHAR2, --返回值
                               o_msg      OUT VARCHAR2, --消息
                               o_list_cur OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_prepare_batch(o_ret_code     OUT VARCHAR2, --返回值
                               o_msg          OUT VARCHAR2, --消息
                               o_batchid_list OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_batch_sync(i_batch_id IN VARCHAR2,
                            o_ret_code OUT VARCHAR2,
                            o_msg      OUT VARCHAR2);
  
END pckg_dams_todo_count_send;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_todo_count_send IS
  FUNCTION func_get_total_count(i_ssic_id IN VARCHAR2 --用户ID
                                ) RETURN VARCHAR2 IS
    v_count VARCHAR2(10) := '0';
  BEGIN
    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT 1
              FROM dams_wf_task t1, dams_wf_process t4
             WHERE t4.sys_code = nvl('', t4.sys_code)
               AND EXISTS
             (SELECT '1'
                      FROM jbpm4_participation t2
                     WHERE t2.userid_ = i_ssic_id
                       AND t2.task_ = t1.task_id)
               AND t1.procinst_id = t4.procinst_id
               AND t1.task_state IN ('open', 'suspended')
               AND nvl(t4.business_type, 0) = nvl('', nvl(t4.business_type, 0))
               AND t4.valid_flag = '1'
            UNION ALL
            SELECT 2
              FROM dams_save_info t,
                   (SELECT t2.pdid, MAX(t2.sys_code) sys_code
                      FROM dams_wf_process_conf t2
                     GROUP BY t2.pdid) t1
             WHERE t.applyer = i_ssic_id
               AND t1.pdid = t.pdid
               AND t1.sys_code = nvl('', t1.sys_code)
               AND nvl(t.biz_type, 0) = nvl('', nvl(t.biz_type, 0))
               AND t.valid_flag = 1
            UNION ALL
            SELECT 3
              FROM dams_outer_todo_task t
             WHERE t.ssic_id = i_ssic_id
               AND t.sys_code = nvl('', t.sys_code)
               AND t.task_state = 'open'
               AND nvl(t.business_type, 0) = nvl('', nvl(t.business_type, 0)));
    RETURN v_count;
  END func_get_total_count;
  
  PROCEDURE proc_insert_users(i_users    IN VARCHAR2, --待办数量变化用户，以$|$分割
                              o_ret_code OUT VARCHAR2 --返回值
                              ) IS
    v_param     db_log.info%TYPE := i_users;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_todo_count_send.proc_insert_users';
    v_step      db_log.step_no%TYPE;
    v_count     NUMBER(5);
    v_strlist   dams_arrytype;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    v_strlist := pckg_dams_util.func_str_to_array(i_users, '$|$', '1');
    FOR i IN 1 .. (v_strlist.count)
    LOOP
      IF v_strlist(i) IS NOT NULL AND length(v_strlist(i)) = 9 THEN
        --软件开发中心
        SELECT COUNT(1)
          INTO v_count
          FROM (SELECT t.stru_id
                  FROM dams_user t, dams_branch_relation br
                 WHERE t.ssic_id = v_strlist(i)
                   AND t.stru_id = br.bnch_id
                   AND br.up_bnch_id = '0010100005'
                UNION ALL
                SELECT t.stru_id
                  FROM dams_user_branch_rel t, dams_branch_relation br
                 WHERE t.ssic_id = v_strlist(i)
                   AND t.stru_id = br.bnch_id
                   AND br.up_bnch_id = '0010100005');
        IF v_count > 0 THEN
          MERGE INTO dams_todo_count_pre_send c
          USING (SELECT v_strlist(i) ssic_id FROM dual) x
          ON (c.ssic_id = x.ssic_id AND c.batch_id IS NULL)
          WHEN MATCHED THEN
            UPDATE SET c.load_time = SYSDATE
          WHEN NOT MATCHED THEN
            INSERT(c.p_key, c.ssic_id, c.load_time)
            VALUES(todo_count_send_seq.nextval, x.ssic_id, SYSDATE);
        END IF;
      END IF;
    END LOOP;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  END proc_insert_users;
  
  PROCEDURE proc_get_assignees(i_task_id  IN VARCHAR2, --当前任务ID
                               o_ret_code OUT VARCHAR2, --返回值
                               o_msg      OUT VARCHAR2, --消息
                               o_list_cur OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_task_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_todo_count_send.proc_get_assignees';
    v_step      db_log.step_no%TYPE;
    v_strlist   dams_arrytype;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    OPEN o_list_cur FOR
      SELECT DISTINCT t.userid_ ssic_id
        FROM jbpm4_participation t
       WHERE t.task_ IN
             (SELECT column_value
                FROM TABLE(CAST(pckg_dams_util.func_str_to_array(i_task_id, ',', '1') AS dams_arrytype))
               WHERE column_value IS NOT NULL)
         AND t.userid_ IS NOT NULL;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      OPEN o_list_cur FOR
        SELECT DISTINCT t.userid_ ssic_id
          FROM jbpm4_participation t
         WHERE 1 <> 1;
      o_ret_code := -1;
  END proc_get_assignees;
  
  PROCEDURE proc_prepare_batch(o_ret_code     OUT VARCHAR2, --返回值
                               o_msg          OUT VARCHAR2, --消息
                               o_batchid_list OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param       db_log.info%TYPE := '';
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_todo_count_send.proc_prepare_batch';
    v_step        db_log.step_no%TYPE;
    v_count       NUMBER(5);
    v_batch_id    NUMBER(20);
    v_batchid_str VARCHAR2(4000) := '';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    UPDATE dams_todo_count_pre_send t
       SET t.batch_id = NULL, t.send_time = NULL, t.fail_reason = NULL
     WHERE t.batch_id IS NOT NULL;
    SELECT ceil(COUNT(1) / 100) INTO v_count FROM dams_todo_count_pre_send;
    IF v_count = 0 THEN
      o_ret_code := '-2';
      o_msg      := 'NO DATA TO BE SEND! ';
      RETURN;
    ELSIF v_count > 10 THEN
      --最多取1000条
      v_count := 10;
    END IF;
    FOR i IN 1 .. v_count
    LOOP
      SELECT todo_count_send_seq.nextval INTO v_batch_id FROM dual;
      UPDATE dams_todo_count_pre_send t
         SET t.batch_id = v_batch_id, t.send_time = SYSDATE
       WHERE t.p_key IN
             (SELECT p_key
                FROM (SELECT t.p_key, row_number() over(ORDER BY t.p_key) rn
                        FROM dams_todo_count_pre_send t
                       WHERE t.batch_id IS NULL)
               WHERE rn <= 100);
      v_batchid_str := v_batchid_str || v_batch_id || ',';
    END LOOP;
    UPDATE dams_todo_count_pre_send ps
       SET ps.total_num = func_get_total_count(ps.ssic_id)
     WHERE ps.batch_id IS NOT NULL;
    OPEN o_batchid_list FOR
      SELECT DISTINCT column_value batch_id
        FROM TABLE(CAST(pckg_dams_util.func_str_to_array(v_batchid_str, ',', '1') AS dams_arrytype))
       WHERE column_value IS NOT NULL;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_prepare_batch;
  
  PROCEDURE proc_batch_sync(i_batch_id IN VARCHAR2,
                            o_ret_code OUT VARCHAR2,
                            o_msg      OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_batch_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_todo_count_send.proc_batch_sync';
    v_step      db_log.step_no%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    INSERT INTO dams_todo_count_aft_send
      (p_key, ssic_id, total_num, batch_id, load_time, send_time, fail_reason)
      SELECT p_key,
             ssic_id,
             total_num,
             batch_id,
             load_time,
             send_time,
             fail_reason
        FROM dams_todo_count_pre_send t
       WHERE t.batch_id = i_batch_id;
    DELETE FROM dams_todo_count_pre_send t WHERE t.batch_id = i_batch_id;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_batch_sync;
  
END pckg_dams_todo_count_send;
//


CREATE OR REPLACE PACKAGE pckg_dams_follow_info IS
  PROCEDURE proc_add(i_ssicid  IN VARCHAR2, --人员编号
                     i_appid   IN VARCHAR2, --业务主键
                     i_flag    IN VARCHAR2, --类型0-申请，1-文档
                     o_retcode OUT VARCHAR2,
                     o_msg     OUT VARCHAR2);
  
  PROCEDURE proc_remove(i_ssicid  IN VARCHAR2, --人员编号
                        i_appid   IN VARCHAR2, --业务主键
                        i_flag    IN VARCHAR2, --类型0-申请，1-文档
                        o_retcode OUT VARCHAR2,
                        o_msg     OUT VARCHAR2);
  
  PROCEDURE proc_isfollow(i_ssicid   IN VARCHAR2, --人员编号
                          i_appid    IN VARCHAR2, --业务主键
                          i_flag     IN VARCHAR2, --类型0-申请，1-文档
                          o_retcode  OUT VARCHAR2,
                          o_msg      OUT VARCHAR2,
                          o_isfollow OUT VARCHAR2);
  
  PROCEDURE proc_query(i_ssid       IN VARCHAR2,
                       i_sys_code   IN VARCHAR2,
                       i_biz_type   IN VARCHAR2,
                       i_label      IN VARCHAR2, --标签
                       i_beg_num    IN VARCHAR2,
                       i_fetch_num  IN VARCHAR2,
                       i_condition  IN VARCHAR2, --模糊查询条件
                       o_retcode    OUT VARCHAR2,
                       o_msg        OUT VARCHAR2,
                       o_followlist OUT SYS_REFCURSOR,
                       o_total_num  OUT VARCHAR2 --记录总数
                       );
  
  PROCEDURE proc_taskid(i_businessid IN VARCHAR2,
                        o_retcode    OUT VARCHAR2,
                        o_msg        OUT VARCHAR2,
                        o_taskid     OUT VARCHAR2);
  
  PROCEDURE proc_find_doc_follow_url(i_appid    IN VARCHAR2, --业务主键
                                     i_flag     IN VARCHAR2, --类型0-申请，1-文档
                                     o_retcode  OUT VARCHAR2,
                                     o_msg      OUT VARCHAR2,
                                     o_form_url OUT VARCHAR2);
  
  PROCEDURE proc_save_label(i_ssicid  IN VARCHAR2, --人员编号
                            i_appid   IN VARCHAR2, --业务主键
                            i_label   IN VARCHAR2,
                            o_retcode OUT VARCHAR2,
                            o_msg     OUT VARCHAR2);
  
  PROCEDURE proc_query_label(i_ssicid     IN VARCHAR2,
                             i_sys_code   IN VARCHAR2,
                             i_biz_type   IN VARCHAR2,
                             o_retcode    OUT VARCHAR2,
                             o_total_num  OUT VARCHAR2,
                             o_label_list OUT SYS_REFCURSOR);
  
  PROCEDURE proc_del_label(i_ssicid  IN VARCHAR2,
                           i_appid   IN VARCHAR2,
                           i_label   IN VARCHAR2,
                           o_retcode OUT VARCHAR2);
  
END pckg_dams_follow_info;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_follow_info IS
  PROCEDURE proc_add(i_ssicid  IN VARCHAR2, --人员编号
                     i_appid   IN VARCHAR2, --业务主键
                     i_flag    IN VARCHAR2, --类型0-申请，1-文档
                     o_retcode OUT VARCHAR2,
                     o_msg     OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssicid || '|' || i_appid || '|' || i_flag;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_add';
    v_step      db_log.step_no%TYPE;
    v_biztype   VARCHAR2(10);
    v_info      VARCHAR2(600);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    IF i_flag = '0' THEN
      SELECT t.business_type, t.title
        INTO v_biztype, v_info
        FROM viw_dams_wf_all_process t
       WHERE business_id = i_appid
         AND rownum = 1;
    ELSIF i_flag = '1' THEN
      IF i_appid LIKE 'SJ%' OR upper(i_appid) LIKE 'ZFWJ%' THEN
        v_biztype := 'SJ01';
        SELECT t.title
          INTO v_info
          FROM sj_gov_file_info t
         WHERE t.app_id = i_appid;
      END IF;
    END IF;
    INSERT INTO dams_follow_info(ssic_id, app_id, biz_type, sys_code, save_time, info, flag)
    VALUES
      (i_ssicid,
       i_appid,
       v_biztype,
       substr(v_biztype, 0, 2),
       SYSDATE,
       v_info,
       i_flag);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line(SQLERRM);
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_add;
  
  PROCEDURE proc_remove(i_ssicid  IN VARCHAR2, --人员编号
                        i_appid   IN VARCHAR2, --业务主键
                        i_flag    IN VARCHAR2, --类型0-申请，1-文档
                        o_retcode OUT VARCHAR2,
                        o_msg     OUT VARCHAR2) IS
    v_param      db_log.info%TYPE := i_ssicid || '|' || i_appid || '|' || i_flag;
    v_proc_name  db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_remove';
    v_step       db_log.step_no%TYPE;
    v_appid_list dams_arrytype;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    IF i_appid IS NOT NULL THEN
      v_appid_list := pckg_dams_util.func_str_to_array(i_appid, ',', '1');
      FOR i IN 1 .. (v_appid_list.count)
      LOOP
        DELETE FROM dams_follow_info t
         WHERE t.ssic_id = i_ssicid
           AND t.app_id = v_appid_list(i);
      END LOOP;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line(SQLERRM);
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_remove;
  
  PROCEDURE proc_isfollow(i_ssicid   IN VARCHAR2, --人员编号
                          i_appid    IN VARCHAR2, --业务主键
                          i_flag     IN VARCHAR2, --类型0-申请，1-文档
                          o_retcode  OUT VARCHAR2,
                          o_msg      OUT VARCHAR2,
                          o_isfollow OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssicid || '|' || i_appid || '|' || i_flag;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_isfollow';
    v_step      db_log.step_no%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    SELECT COUNT(1)
      INTO o_isfollow
      FROM dams_follow_info t
     WHERE t.ssic_id = i_ssicid
       AND t.app_id = i_appid
       AND t.flag = i_flag;
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line(SQLERRM);
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_isfollow;
  
  PROCEDURE proc_query(i_ssid       IN VARCHAR2,
                       i_sys_code   IN VARCHAR2,
                       i_biz_type   IN VARCHAR2,
                       i_label      IN VARCHAR2, --标签
                       i_beg_num    IN VARCHAR2,
                       i_fetch_num  IN VARCHAR2,
                       i_condition  IN VARCHAR2, --模糊查询条件
                       o_retcode    OUT VARCHAR2,
                       o_msg        OUT VARCHAR2,
                       o_followlist OUT SYS_REFCURSOR,
                       o_total_num  OUT VARCHAR2 --记录总数
                       ) IS

    v_param     db_log.info%TYPE := i_ssid || '|' || i_sys_code || '|' || i_biz_type || '|' || i_label || '|' || i_beg_num || '|' || i_fetch_num || '|' || i_condition;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_query';
    v_step      db_log.step_no%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    IF i_label IS NULL THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT t.info,
                     --pckg_dams_util.FUNC_DECODE('BIZ_TYPE', t.biz_type, 'DAMS') biz_type,
                     t.ssic_id,
                     t.app_id,
                     t.sys_code,
                     t.flag,
                     --t.out_flag,
                     nvl2(t1.sysurl,
                          t1.sysurl || '/' || t1.context || '/' || t.outer_url, '') outer_url,
                     nvl(c.sys_sname, '办公信息化系统') sys_sname,
                     nvl2(t1.sysurl, t1.sysurl || '/' || t1.context || '/', '') pre_url,
                     (SELECT listagg(dfl.label ||
                                     '<img style="cursor:hand;width: 14px;height: 14px;margin:1px 0px;top:4px" src="/icbc/ois/images/icons/delete.gif" onClick="delLabel(''' ||
                                     dfl.app_id || ''',''' || dfl.label ||
                                     ''')"/>', ',') within GROUP(ORDER BY 1)
                        FROM dams_follow_label dfl
                       WHERE t.app_id = dfl.app_id(+)
                         AND dfl.ssic_id = i_ssid) tag,
                     (SELECT listagg(dfl.label, ',') within GROUP(ORDER BY 1)
                        FROM dams_follow_label dfl
                       WHERE t.app_id = dfl.app_id(+)
                         AND dfl.ssic_id = i_ssid) tag_src,
                     t.save_time
                FROM dams_follow_info t, dams_sys_url_cfg t1, dams_sys_conf c
               WHERE t.ssic_id = i_ssid
                 AND t.sys_code = t1.sys_code(+)
                    --and  t1.disable(+) = '0'
                 AND t.sys_code = c.sys_code(+)
                 AND t.sys_code = nvl(i_sys_code, t.sys_code)
                 AND t.biz_type = nvl(i_biz_type, t.biz_type)) tmp
       WHERE (tmp.info LIKE '%' || i_condition || '%' OR
             tmp.tag_src LIKE '%' || i_condition || '%');
      OPEN o_followlist FOR
        SELECT info,
               ssic_id,
               app_id,
               sys_code,
               flag,
               outer_url,
               sys_sname,
               pre_url,
               tag
          FROM (SELECT tmp.info,
                       tmp.ssic_id,
                       tmp.app_id,
                       tmp.sys_code,
                       tmp.flag,
                       tmp.outer_url,
                       tmp.sys_sname,
                       tmp.pre_url,
                       tmp.tag,
                       row_number() over(ORDER BY tmp.save_time DESC) rn
                  FROM (SELECT t.info,
                               --pckg_dams_util.FUNC_DECODE('BIZ_TYPE', t.biz_type, 'DAMS') biz_type,
                               t.ssic_id,
                               t.app_id,
                               t.sys_code,
                               t.flag,
                               --t.out_flag,
                               nvl2(t1.sysurl,
                                    t1.sysurl || '/' || t1.context || '/' ||
                                     t.outer_url, '') outer_url,
                               nvl(c.sys_sname, '办公信息化系统') sys_sname,
                               nvl2(t1.sysurl,
                                    t1.sysurl || '/' || t1.context || '/', '') pre_url,
                               (SELECT listagg(dfl.label ||
                                               '<img style="cursor:hand;width: 14px;height: 14px;margin:1px 0px;top:4px" src="/icbc/ois/images/icons/delete.gif" onClick="delLabel(''' ||
                                               dfl.app_id || ''',''' || dfl.label ||
                                               ''')"/>', ',') within GROUP(ORDER BY 1)
                                  FROM dams_follow_label dfl
                                 WHERE t.app_id = dfl.app_id(+)
                                   AND dfl.ssic_id = i_ssid) tag,
                                (SELECT listagg(dfl.label , ',') within GROUP(ORDER BY 1)
                                  FROM dams_follow_label dfl
                                 WHERE t.app_id = dfl.app_id(+)
                                   AND dfl.ssic_id = i_ssid) tag_src,
                               t.save_time

                          FROM dams_follow_info t,
                               dams_sys_url_cfg t1,
                               dams_sys_conf    c
                         WHERE t.ssic_id = i_ssid
                           AND t.sys_code = t1.sys_code(+)
                              --and  t1.disable(+) = '0'
                           AND t.sys_code = c.sys_code(+)
                           AND t.sys_code = nvl(i_sys_code, t.sys_code)
                           AND t.biz_type = nvl(i_biz_type, t.biz_type)) tmp
                 WHERE (tmp.info LIKE '%' || i_condition || '%' OR
                       tmp.tag_src LIKE '%' || i_condition || '%'))
         WHERE rn >= to_number(i_beg_num)
           AND rn <= to_number(i_beg_num) + to_number(i_fetch_num) - 1;
    ELSE
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT t.info,
                     --pckg_dams_util.FUNC_DECODE('BIZ_TYPE', t.biz_type, 'DAMS') biz_type,
                     t.ssic_id,
                     t.app_id,
                     t.sys_code,
                     t.flag,
                     --t.out_flag,
                     nvl2(t1.sysurl,
                          t1.sysurl || '/' || t1.context || '/' || t.outer_url, '') outer_url,
                     nvl(c.sys_sname, '办公信息化系统') sys_sname,
                     nvl2(t1.sysurl, t1.sysurl || '/' || t1.context || '/', '') pre_url,
                     (SELECT listagg(dfl.label ||
                                     '<img style="cursor:hand;width: 14px;height: 14px;margin:1px 0px;top:4px" src="/icbc/ois/images/icons/delete.gif" onClick="delLabel(''' ||
                                     dfl.app_id || ''',''' || dfl.label || ''')"/>',
                                     ',') within GROUP(ORDER BY 1)
                        FROM dams_follow_label dfl
                       WHERE t.app_id = dfl.app_id(+)
                         AND dfl.ssic_id = i_ssid) tag,
                     (SELECT listagg(dfl.label, ',') within GROUP(ORDER BY 1)
                        FROM dams_follow_label dfl
                       WHERE t.app_id = dfl.app_id(+)
                         AND dfl.ssic_id = i_ssid) tag_src,
                     t.save_time

                FROM dams_follow_info t, dams_sys_url_cfg t1, dams_sys_conf c
               WHERE t.ssic_id = i_ssid
                 AND t.sys_code = t1.sys_code(+)
                    --and  t1.disable(+) = '0'
                 AND t.sys_code = c.sys_code(+)
                 AND t.sys_code = nvl(i_sys_code, t.sys_code)
                 AND t.biz_type = nvl(i_biz_type, t.biz_type)
                 AND t.app_id IN (SELECT dfl.app_id
                                    FROM dams_follow_label dfl
                                   WHERE dfl.ssic_id = i_ssid
                                     AND dfl.label = i_label)) tmp
       WHERE (tmp.info LIKE '%' || i_condition || '%' OR
             tmp.tag_src LIKE '%' || i_condition || '%');
      OPEN o_followlist FOR
        SELECT info,
               ssic_id,
               app_id,
               sys_code,
               flag,
               outer_url,
               sys_sname,
               pre_url,
               tag
          FROM (SELECT tmp.info,
                       tmp.ssic_id,
                       tmp.app_id,
                       tmp.sys_code,
                       tmp.flag,
                       tmp.outer_url,
                       tmp.sys_sname,
                       tmp.pre_url,
                       tmp.tag,
                       row_number() over(ORDER BY tmp.save_time DESC) rn
                  FROM (SELECT t.info,
                               --pckg_dams_util.FUNC_DECODE('BIZ_TYPE', t.biz_type, 'DAMS') biz_type,
                               t.ssic_id,
                               t.app_id,
                               t.sys_code,
                               t.flag,
                               --t.out_flag,
                               nvl2(t1.sysurl,
                                    t1.sysurl || '/' || t1.context || '/' ||
                                     t.outer_url, '') outer_url,
                               nvl(c.sys_sname, '办公信息化系统') sys_sname,
                               nvl2(t1.sysurl,
                                    t1.sysurl || '/' || t1.context || '/', '') pre_url,
                               (SELECT listagg(dfl.label ||
                                               '<img style="cursor:hand;width: 14px;height: 14px;margin:1px 0px;top:4px" src="/icbc/ois/images/icons/delete.gif" onClick="delLabel(''' ||
                                               dfl.app_id || ''',''' || dfl.label ||
                                               ''')"/>', ',') within GROUP(ORDER BY 1)
                                  FROM dams_follow_label dfl
                                 WHERE t.app_id = dfl.app_id(+)
                                   AND dfl.ssic_id = i_ssid) tag,
                                (SELECT listagg(dfl.label , ',') within GROUP(ORDER BY 1)
                                  FROM dams_follow_label dfl
                                 WHERE t.app_id = dfl.app_id(+)
                                   AND dfl.ssic_id = i_ssid) tag_src,
                               t.save_time
                          FROM dams_follow_info t,
                               dams_sys_url_cfg t1,
                               dams_sys_conf    c
                         WHERE t.ssic_id = i_ssid
                           AND t.sys_code = t1.sys_code(+)
                              --and  t1.disable(+) = '0'
                           AND t.sys_code = c.sys_code(+)
                           AND t.sys_code = nvl(i_sys_code, t.sys_code)
                           AND t.biz_type = nvl(i_biz_type, t.biz_type)
                           AND t.app_id IN (
                              SELECT dfl.app_id
                                FROM dams_follow_label dfl
                               WHERE dfl.ssic_id = i_ssid
                                 AND dfl.label = i_label)
                           ) tmp
                 WHERE (tmp.info LIKE '%' || i_condition || '%' OR
                       tmp.tag_src LIKE '%' || i_condition || '%'))
         WHERE rn >= to_number(i_beg_num)
           AND rn <= to_number(i_beg_num) + to_number(i_fetch_num) - 1;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      IF o_followlist%ISOPEN THEN
        CLOSE o_followlist;
      END IF;
  END proc_query;
  
  PROCEDURE proc_taskid(i_businessid IN VARCHAR2,
                        o_retcode    OUT VARCHAR2,
                        o_msg        OUT VARCHAR2,
                        o_taskid     OUT VARCHAR2) IS

    v_param     db_log.info%TYPE := i_businessid;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_taskID';
    v_step      db_log.step_no%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    SELECT MAX(task_id) INTO o_taskid FROM dams_wf_task WHERE business_id = i_businessid;
    IF o_taskid IS NULL THEN
      SELECT MAX(task_id)
        INTO o_taskid
        FROM dams_wf_hist_task
       WHERE business_id = i_businessid;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      --dbms_output.put_line(SQLERRM);
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_taskid;
  
  PROCEDURE proc_find_doc_follow_url(i_appid    IN VARCHAR2, --业务主键
                                     i_flag     IN VARCHAR2, --类型0-申请，1-文档
                                     o_retcode  OUT VARCHAR2,
                                     o_msg      OUT VARCHAR2,
                                     o_form_url OUT VARCHAR2) IS
    -- exception variables
    v_proc_name db_log.proc_name%TYPE; -- procedure name
    v_step_no   db_log.step_no%TYPE; -- step
  BEGIN
    v_step_no   := '1';
    v_proc_name := 'pckg_dams_follow_info.proc_find_doc_follow_url';
    o_retcode   := 0;
    IF i_appid LIKE 'SJ%' OR upper(i_appid) LIKE 'ZFWJ%' THEN
      o_form_url := '/govfile/sj_gov_file_common_view_detail.jsp';
    ELSE
      o_form_url := '';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        o_msg := v_proc_name || '模块内部错误:' || o_msg || SQLERRM;
        pack_log.log(v_proc_name, v_step_no, o_msg, '4');
        o_retcode := -1;
      END;
  END proc_find_doc_follow_url;
  
  PROCEDURE proc_save_label(i_ssicid  IN VARCHAR2, --人员编号
                            i_appid   IN VARCHAR2, --业务主键
                            i_label   IN VARCHAR2,
                            o_retcode OUT VARCHAR2,
                            o_msg     OUT VARCHAR2) IS

    v_param      db_log.info%TYPE := i_ssicid || '|' || i_appid || '|' || i_label;
    v_proc_name  db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_save_label';
    v_step       db_log.step_no%TYPE;
    v_appid_list dams_arrytype;
    v_count NUMBER;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;

    IF i_appid IS NOT NULL THEN
      v_appid_list := pckg_dams_util.func_str_to_array(i_appid, ',', '1');
      FOR i IN 1 .. (v_appid_list.count)
      LOOP
        --一个人一条关注，是否已有该标签
        SELECT COUNT(1)
          INTO v_count
          FROM dams_follow_label t
         WHERE t.ssic_id = i_ssicid
           AND t.app_id = v_appid_list(i)
           AND t.label = i_label;
        IF v_count = 0 THEN
          INSERT INTO dams_follow_label
            (ssic_id, app_id, label)
          VALUES
            (i_ssicid, v_appid_list(i), i_label);
        END IF;
      END LOOP;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_save_label;
  
  PROCEDURE proc_query_label(i_ssicid     IN VARCHAR2,
                             i_sys_code   IN VARCHAR2,
                             i_biz_type   IN VARCHAR2,
                             o_retcode    OUT VARCHAR2,
                             o_total_num  OUT VARCHAR2,
                             o_label_list OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssicid || '|' || i_sys_code || '|' || i_biz_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_query_label';
    v_step      db_log.step_no%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    SELECT COUNT(DISTINCT t1.label)
      INTO o_total_num
      FROM dams_follow_label t1, dams_follow_info t2
     WHERE t1.app_id = t2.app_id
       AND t1.ssic_id = i_ssicid
       AND t2.sys_code = nvl(i_sys_code, t2.sys_code)
       AND t2.biz_type = nvl(i_biz_type, t2.biz_type)
     ORDER BY t1.label;
    OPEN o_label_list FOR
      SELECT DISTINCT t1.label tag
        FROM dams_follow_label t1, dams_follow_info t2
       WHERE t1.app_id = t2.app_id
         AND t1.ssic_id = i_ssicid
         AND t2.sys_code = nvl(i_sys_code, t2.sys_code)
         AND t2.biz_type = nvl(i_biz_type, t2.biz_type)
       ORDER BY t1.label;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      IF o_label_list%ISOPEN THEN
        CLOSE o_label_list;
      END IF;
  END proc_query_label;
  
  PROCEDURE proc_del_label(i_ssicid  IN VARCHAR2,
                           i_appid   IN VARCHAR2,
                           i_label   IN VARCHAR2,
                           o_retcode OUT VARCHAR2) IS

    v_param     db_log.info%TYPE := i_ssicid || '|' || i_appid || '|' || i_label;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_follow_info.proc_del_label';
    v_step      db_log.step_no%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := 0;
    DELETE FROM dams_follow_label t
     WHERE t.ssic_id = i_ssicid
       AND t.app_id = i_appid
       AND t.label = i_label;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
      ROLLBACK;
  END proc_del_label;
  
END pckg_dams_follow_info;
//

CREATE OR REPLACE PACKAGE pckg_branch_selector IS
  PROCEDURE proc_get_simple_tree(i_stru_id  IN VARCHAR2, --申请人
                                 i_level    IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR);

  PROCEDURE proc_get_rules(i_stru_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR);

END pckg_branch_selector;
//

CREATE OR REPLACE PACKAGE BODY pckg_branch_selector IS
  PROCEDURE proc_get_simple_tree(i_stru_id  IN VARCHAR2,
                                 i_level    IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_stru_id || '|' || i_level;
    v_proc_name db_log.proc_name%TYPE := 'pckg_branch_selector.proc_get_simple_tree';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    OPEN o_list FOR
      SELECT LEVEL,
             t.stru_id,
             t.stru_fname,
             t.stru_sname,
             t.stru_sign,
             t.sup_stru,
             t.man_grade,
             t.stru_sort,
             t.stru_lv,
             t.stru_grade
        FROM dams_branch t
       START WITH stru_id = i_stru_id
      CONNECT BY PRIOR stru_id = sup_stru
             AND stru_state IN ('1', '2')
             AND LEVEL <= i_level
       ORDER SIBLINGS BY stru_sort DESC NULLS LAST;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_get_simple_tree;
  
  PROCEDURE proc_get_rules(i_stru_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_stru_id || '|' || i_func;
    v_proc_name db_log.proc_name%TYPE := 'pckg_branch_selector.proc_get_rules';
    v_count NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    SELECT COUNT(1)
      INTO v_count
      FROM dams_stru_select_range t
     WHERE t.func = i_func
       AND stru_id = i_stru_id;
    IF v_count = 0 THEN
      OPEN o_list FOR
        SELECT t.rule_id, t.rule_type, t.rule_content, t.max_level
          FROM dams_stru_select_range t
         WHERE t.func = i_func
           AND stru_id = 'default'
         ORDER BY t.rule_id;
    ELSE
      OPEN o_list FOR
        SELECT t.rule_id, t.rule_type, t.rule_content, t.max_level
          FROM dams_stru_select_range t
         WHERE t.func = i_func
           AND stru_id = i_stru_id
         ORDER BY t.rule_id;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_get_rules;
  
END pckg_branch_selector;
//

CREATE OR REPLACE PACKAGE pckg_ois_group IS
  PROCEDURE proc_add_group(i_ssic_id     IN VARCHAR2,
                           i_func        IN VARCHAR2, --功能类型
                           i_group_name  IN VARCHAR2, --群组名称
                           i_group_value IN VARCHAR2, --群组值
                           o_ret_code    OUT VARCHAR2,
                           o_msg         OUT VARCHAR2);
  
  PROCEDURE proc_delete_group(i_group_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2,
                              o_msg      OUT VARCHAR2);
  
  PROCEDURE proc_qry_group(i_ssic_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_msg      OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR);
  
END pckg_ois_group;
//


CREATE OR REPLACE PACKAGE BODY pckg_ois_group IS
  PROCEDURE proc_add_group(i_ssic_id     IN VARCHAR2,
                           i_func        IN VARCHAR2, --功能类型
                           i_group_name  IN VARCHAR2, --群组名称
                           i_group_value IN VARCHAR2, --群组值
                           o_ret_code    OUT VARCHAR2,
                           o_msg         OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_func || '|' || i_group_name || '|' || i_group_value;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_add_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    SELECT COUNT(1)
      INTO v_count
      FROM ois_group t
     WHERE t.group_name = i_group_name
       AND t.ssic_id = i_ssic_id
       AND t.func = i_func;
    IF v_count > 0 THEN
      SELECT t.group_id
        INTO v_group_id
        FROM ois_group t
       WHERE t.group_name = i_group_name
         AND t.ssic_id = i_ssic_id
         AND t.func = i_func;
      DELETE FROM ois_group_content t WHERE t.group_id = v_group_id;
      INSERT INTO ois_group_content t
        SELECT v_group_id, column_value, rownum
          FROM TABLE(pckg_dams_util.func_str_to_array(i_group_value, ','));
    ELSE
      v_group_id := pckg_dams_util.func_gen_seq('GP', 'GROUP_SEQ');
      INSERT INTO ois_group
      VALUES (v_group_id, i_group_name, i_ssic_id, i_func, i_ssic_id, SYSDATE);
      INSERT INTO ois_group_content t
        SELECT v_group_id, column_value, rownum
          FROM TABLE(pckg_dams_util.func_str_to_array(i_group_value, ','));
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_add_group;
  
  PROCEDURE proc_delete_group(i_group_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2,
                              o_msg      OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_group_id;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_delete_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    DELETE FROM ois_group t WHERE t.group_id = i_group_id;
    DELETE FROM ois_group_content t WHERE t.group_id = i_group_id;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_delete_group;
  
  PROCEDURE proc_qry_group(i_ssic_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_msg      OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_func;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_qry_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := 0;
    OPEN o_list FOR
      SELECT t.group_name title,
             t.group_id key,
             listagg(t1.group_content, ',') within GROUP(ORDER BY t1.sort) VALUE
        FROM ois_group t, ois_group_content t1
       WHERE t.group_id = t1.group_id
         AND t.ssic_id = i_ssic_id
         AND t.func = i_func
       GROUP BY t.group_id, t.group_name;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_qry_group;
  
END pckg_ois_group;
//


CREATE OR REPLACE PACKAGE ctpext_pkg_file IS
  TYPE ref_cur IS REF CURSOR;

  PROCEDURE proc_file_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file_name        IN VARCHAR2, --文件名
                             i_file_data        IN BLOB, --文件blob数据
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2);
  
   PROCEDURE proc_new_file_ref(i_user_id     IN VARCHAR2,
                              i_fileref_seq IN VARCHAR2, --文件编号序列器
                              o_retcode     OUT VARCHAR2,
                              o_ref_id      OUT VARCHAR2);
   
   PROCEDURE proc_file_download(i_user_id          IN VARCHAR2,
                               i_table_name       IN VARCHAR2, --存储文件表表名
                               i_fileid_colname   IN VARCHAR2, --文件id字段名
                               i_file_id          IN VARCHAR2, --需要下载的文件id
                               i_filename_colname IN VARCHAR2, --文件名字段名
                               i_suff_colname     IN VARCHAR2, --后缀字段名
                               i_blob_colname     IN VARCHAR2, --文件blob字段名
                               o_retcode          OUT VARCHAR2,
                               o_file_name        OUT VARCHAR2, --需要下载的文件名
                               o_file_suff        OUT VARCHAR2, --需要下载的文件后缀
                               o_file_data        OUT BLOB);
   
   PROCEDURE proc_file_list(i_user_id          IN VARCHAR2,
                           i_table_name       IN VARCHAR2, --存储文件表表名
                           i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                           i_ref_id           IN VARCHAR2, --文件引用编号
                           i_fileid_colname   IN VARCHAR2, --文件id字段名
                           i_filename_colname IN VARCHAR2, --文件名字段名
                           i_suff_colname     IN VARCHAR2, --后缀字段名
                           i_ext_colname      IN VARCHAR2, --额外的字段名
                           i_valid_also       IN VARCHAR2, --是否只取有效数据
                           o_retcode          OUT VARCHAR2,
                           o_total_num        OUT VARCHAR2, --文件个数
                           o_file_detail      OUT ref_cur);
   
   PROCEDURE proc_file_delete(i_user_id        IN VARCHAR2,
                             i_table_name     IN VARCHAR2, --存储文件表表名
                             i_fileid_colname IN VARCHAR2, --文件id字段名
                             i_file_ids       IN VARCHAR2, --需要删除的的文件id序列 例如1，2，3
                             o_retcode        OUT VARCHAR2);
   
   PROCEDURE proc_file_updateflag(i_user_id        IN VARCHAR2,
                                 i_table_name     IN VARCHAR2, --存储文件表表名
                                 i_fileid_colname IN VARCHAR2, --文件id字段名
                                 i_file_ids       IN VARCHAR2, --需要更新的的文件id序列 例如1，2，3
                                 i_flag_colname   IN VARCHAR2,
                                 i_flag_value     IN VARCHAR2,
                                 o_retcode        OUT VARCHAR2);
   
   PROCEDURE proc_files_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file1_name        IN VARCHAR2, --文件名
                             i_file1_data        IN BLOB, --文件blob数据
                             i_file2_name        IN VARCHAR2, --文件名
                             i_file2_data        IN BLOB, --文件blob数据
                             i_file3_name        IN VARCHAR2, --文件名
                             i_file3_data        IN BLOB, --文件blob数据
                             i_file4_name        IN VARCHAR2, --文件名
                             i_file4_data        IN BLOB, --文件blob数据
                             i_file5_name        IN VARCHAR2, --文件名
                             i_file5_data        IN BLOB, --文件blob数据                 
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2);
   
END ctpext_pkg_file;
//

CREATE OR REPLACE PACKAGE BODY ctpext_pkg_file IS
  PROCEDURE proc_file_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file_name        IN VARCHAR2, --文件名
                             i_file_data        IN BLOB, --文件blob数据
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表明
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2) AS

    v_dynamic_sql VARCHAR2(4000);
    v_userid_colname   VARCHAR2(50);
    v_updtime_colname  VARCHAR2(50);
    v_isvalid_colname  VARCHAR2(50);
    i_ext_colname_temp VARCHAR2(1000);
    empty_data EXCEPTION;
    empty_field EXCEPTION;
    error_extcol EXCEPTION;
    v_num_temp NUMBER(10);
    v_point    NUMBER(10);
    v_file_id   VARCHAR2(50);
    v_file_suff VARCHAR2(100);
    v_msg       VARCHAR2(1000);
  BEGIN
    --初始化处理日志的相关参数
    IF i_ref_id IS NULL OR i_file_name IS NULL THEN
      --OR i_file_data IS NULL
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;
    IF i_table_name IS NULL OR i_fileid_colname IS NULL OR
       i_filename_colname IS NULL OR i_fileref_colname IS NULL OR
       i_blob_colname IS NULL OR i_suff_colname IS NULL OR
       i_fileid_seq IS NULL OR i_ext_colname IS NULL THEN
      v_msg := '部分文件存储表表数据为空';
      RAISE empty_field;
    END IF;
    --分析数据，分析额外字段名，以“,”分割
    i_ext_colname_temp := i_ext_colname || ',';
    v_point            := instr(i_ext_colname_temp, ',');
    v_userid_colname   := substr(i_ext_colname_temp, 1, v_point - 1); --操作用户ID字段名
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;
    v_point            := instr(i_ext_colname_temp, ',');
    v_updtime_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;
    v_point            := instr(i_ext_colname_temp, ',');
    v_isvalid_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    v_file_suff := substr(i_file_name, instr(i_file_name, '.', -1, 1) + 1);
    v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') || ''' || lpad(' || i_fileid_seq || '.nextval,10,0) FROM dual';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO v_file_id;
    v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                     i_fileid_colname || ',' || i_filename_colname || ',' ||
                     i_suff_colname || ',' || i_fileref_colname || ',' ||
                     v_userid_colname || ',' || v_updtime_colname || ',' ||
                     i_blob_colname || ',' || v_isvalid_colname ||
                     ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
    EXECUTE IMMEDIATE v_dynamic_sql
      USING v_file_id, i_file_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file_data, '0';
    o_file_id := v_file_id;
    o_retcode := '0';
  EXCEPTION
    WHEN empty_data THEN
      ROLLBACK;
      o_retcode := '-10';
    WHEN empty_field THEN
      ROLLBACK;
      o_retcode := '-11';
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
  END proc_file_upload;
  
  PROCEDURE proc_new_file_ref(i_user_id     IN VARCHAR2,
                              i_fileref_seq IN VARCHAR2, --文件编号序列器
                              o_retcode     OUT VARCHAR2,
                              o_ref_id      OUT VARCHAR2) AS
    v_dynamic_sql VARCHAR2(4000);
    v_msg         VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                     ''' ||lpad(' || i_fileref_seq ||
                     '.nextval,10,0) FROM dual';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO o_ref_id;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
  END proc_new_file_ref;
  
  PROCEDURE proc_file_download(i_user_id          IN VARCHAR2,
                               i_table_name       IN VARCHAR2, --存储文件表表名
                               i_fileid_colname   IN VARCHAR2, --文件id字段名
                               i_file_id          IN VARCHAR2, --需要下载的文件id
                               i_filename_colname IN VARCHAR2, --文件名字段名
                               i_suff_colname     IN VARCHAR2, --后缀字段名
                               i_blob_colname     IN VARCHAR2, --文件blob字段名
                               o_retcode          OUT VARCHAR2,
                               o_file_name        OUT VARCHAR2, --需要下载的文件名
                               o_file_suff        OUT VARCHAR2, --需要下载的文件后缀
                               o_file_data        OUT BLOB) AS

    v_dynamic_sql VARCHAR2(4000);
    v_msg         VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    v_dynamic_sql := ' SELECT ' || i_filename_colname || ',' ||
                     i_suff_colname || ',' || i_blob_colname || ' FROM ' ||
                     i_table_name || ' WHERE ' || i_fileid_colname || '=''' ||
                     i_file_id || '''';
    EXECUTE IMMEDIATE v_dynamic_sql INTO o_file_name, o_file_suff, o_file_data;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
  END proc_file_download;
  
  PROCEDURE proc_file_list(i_user_id          IN VARCHAR2,
                           i_table_name       IN VARCHAR2, --存储文件表表名
                           i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                           i_ref_id           IN VARCHAR2, --文件引用编号
                           i_fileid_colname   IN VARCHAR2, --文件id字段名
                           i_filename_colname IN VARCHAR2, --文件名字段名
                           i_suff_colname     IN VARCHAR2, --后缀字段名
                           i_ext_colname      IN VARCHAR2, --额外的字段名
                           i_valid_also       IN VARCHAR2, --是否只取有效数据
                           o_retcode          OUT VARCHAR2,
                           o_total_num        OUT VARCHAR2, --文件个数
                           o_file_detail      OUT ref_cur) AS
    v_msg         VARCHAR2(4000);
    v_dynamic_sql VARCHAR2(4000);
    v_is_valid    VARCHAR2(10);
    v_ext_colname VARCHAR2(200);
    v_p NUMBER(3, 0);
    v_q NUMBER(3, 0);
    error_extcol EXCEPTION;
  BEGIN
    --初始化处理日志的相关参数
    v_ext_colname := i_ext_colname || ',';
    v_p           := instr(v_ext_colname, ',', 1, 2);
    v_q           := instr(v_ext_colname, ',', 1, 3);
    IF v_p IS NULL OR v_q IS NULL THEN
      RAISE error_extcol;
    END IF;
    v_is_valid := substr(v_ext_colname, v_p + 1, v_q - v_p - 1);
    IF i_valid_also != '1' THEN
      --删除无效数据
      v_dynamic_sql := 'DELETE FROM ' || i_table_name || ' WHERE ' ||
                       i_fileref_colname || '=:1 AND ' || v_is_valid ||
                       ' = :2 ';
      EXECUTE IMMEDIATE v_dynamic_sql USING i_ref_id, '0';
    END IF;
    v_dynamic_sql := 'SELECT COUNT(1) FROM ' || i_table_name || ' WHERE ' ||
                     i_fileref_colname || '= :1 ';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO o_total_num
      USING i_ref_id;
    v_dynamic_sql := ' SELECT ' || i_fileid_colname || ' FILE_ID,' ||
                     i_fileref_colname || ' REF_ID,' || i_filename_colname ||
                     ' FILE_NAME,' || i_suff_colname || ' FILE_SUFF' ||
                     ' FROM ' || i_table_name || ' WHERE ' || i_fileref_colname ||
                     '= :1 AND ' || v_is_valid || ' IS NOT NULL ORDER BY ' ||
                     i_fileid_colname;
    OPEN o_file_detail FOR v_dynamic_sql USING i_ref_id;
    o_retcode := '0';
  EXCEPTION
    WHEN error_extcol THEN
      ROLLBACK;
      o_retcode := '-10';
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
  END proc_file_list;
  
  PROCEDURE proc_file_delete(i_user_id        IN VARCHAR2,
                             i_table_name     IN VARCHAR2, --存储文件表表名
                             i_fileid_colname IN VARCHAR2, --文件id字段名
                             i_file_ids       IN VARCHAR2, --需要删除的的文件id序列 例如1，2，3
                             o_retcode        OUT VARCHAR2) AS
    sqlstr       VARCHAR2(4000);
    v_msg        VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    sqlstr := 'delete from ' || i_table_name || ' where ' || i_fileid_colname || ' in ( ' || i_file_ids || ')';
    EXECUTE IMMEDIATE sqlstr;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
      ROLLBACK;
  END proc_file_delete;
  
  PROCEDURE proc_file_updateflag(i_user_id        IN VARCHAR2,
                                 i_table_name     IN VARCHAR2, --存储文件表表名
                                 i_fileid_colname IN VARCHAR2, --文件id字段名
                                 i_file_ids       IN VARCHAR2, --需要更新的文件id序列 例如1，2，3
                                 i_flag_colname   IN VARCHAR2, ----标识位字段名
                                 i_flag_value     IN VARCHAR2, ---标识值
                                 o_retcode        OUT VARCHAR2) IS
    v_sqlstr     VARCHAR2(4000);
    v_msg        VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    v_sqlstr := 'UPDATE ' || i_table_name || ' SET ' || i_flag_colname ||
                ' = ' || i_flag_value || ' WHERE ' || i_fileid_colname ||
                ' in (' || i_file_ids || ')';
    EXECUTE IMMEDIATE v_sqlstr;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-2';
  END proc_file_updateflag;
  
  PROCEDURE proc_files_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file1_name        IN VARCHAR2, --文件名
                             i_file1_data        IN BLOB, --文件blob数据
                             i_file2_name        IN VARCHAR2, --文件名
                             i_file2_data        IN BLOB, --文件blob数据
                             i_file3_name        IN VARCHAR2, --文件名
                             i_file3_data        IN BLOB, --文件blob数据
                             i_file4_name        IN VARCHAR2, --文件名
                             i_file4_data        IN BLOB, --文件blob数据
                             i_file5_name        IN VARCHAR2, --文件名
                             i_file5_data        IN BLOB, --文件blob数据                         
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2)AS

    v_dynamic_sql VARCHAR2(4000);
    v_userid_colname   VARCHAR2(50);
    v_updtime_colname  VARCHAR2(50);
    v_isvalid_colname  VARCHAR2(50);
    i_ext_colname_temp VARCHAR2(1000);
    empty_data EXCEPTION;
    empty_field EXCEPTION;
    error_extcol EXCEPTION;
    v_num_temp NUMBER(10);
    v_point    NUMBER(10);
    v_file_id   VARCHAR2(50);
    v_file_suff VARCHAR2(100);
    v_msg       VARCHAR2(1000);
    v_proc_name     VARCHAR2(100);
    v_inparam       VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    IF i_ref_id IS NULL OR i_file1_name IS NULL THEN
      --OR i_file_data IS NULL
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;
    v_proc_name := 'ctpext_pkg_file.proc_files_upload';
    v_inparam   := '开始!' || '|' || i_table_name || '|' || i_fileid_colname || '|' ||
                   i_filename_colname || '|' || i_fileref_colname || '|' || i_blob_colname || '|' ||
                   i_suff_colname || '|' || i_fileid_seq || '|' || i_ext_colname;
    pack_log.info(v_proc_name, '', v_inparam);
    IF i_table_name IS NULL OR i_fileid_colname IS NULL OR
       i_filename_colname IS NULL OR i_fileref_colname IS NULL OR
       i_blob_colname IS NULL OR i_suff_colname IS NULL OR
       i_fileid_seq IS NULL OR i_ext_colname IS NULL THEN
      v_msg := '部分文件存储表表数据为空';
      RAISE empty_field;
    END IF;
    --分析数据，分析额外字段名，以“,”分割
    i_ext_colname_temp := i_ext_colname || ',';
    v_point            := instr(i_ext_colname_temp, ',');
    v_userid_colname   := substr(i_ext_colname_temp, 1, v_point - 1); --操作用户ID字段名
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;
    v_point            := instr(i_ext_colname_temp, ',');
    v_updtime_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;
    v_point            := instr(i_ext_colname_temp, ',');
    v_isvalid_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    IF i_file1_name IS NOT NULL THEN
      v_file_suff := substr(i_file1_name, instr(i_file1_name, '.', -1, 1) + 1);
      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;
      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file1_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file1_data, '1';
        o_file_id := v_file_id;
    END IF;
    IF i_file2_name IS NOT NULL THEN
      v_file_suff := substr(i_file2_name, instr(i_file2_name, '.', -1, 1) + 1);
      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;
      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file2_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file2_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;
    IF i_file3_name IS NOT NULL THEN
      v_file_suff := substr(i_file3_name, instr(i_file3_name, '.', -1, 1) + 1);
      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;
      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file3_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file3_data, '1';
      o_file_id := o_file_id || v_file_id;
    END IF;
    IF i_file4_name IS NOT NULL  THEN
          v_file_suff := substr(i_file4_name, instr(i_file4_name, '.', -1, 1) + 1);
      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;
      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file4_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file4_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;
    IF  i_file5_name IS NOT NULL  THEN
      v_file_suff := substr(i_file5_name, instr(i_file5_name, '.', -1, 1) + 1);
      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;
      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file5_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file5_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;
    o_retcode := '0';
  EXCEPTION
    WHEN empty_data THEN
      ROLLBACK;
      o_retcode := '-10';
    WHEN empty_field THEN
      ROLLBACK;
      o_retcode := '-11';
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
  END proc_files_upload;
  
END ctpext_pkg_file;
//

CREATE OR REPLACE PACKAGE pckg_dams_file_upload IS
  TYPE ref_cur IS REF CURSOR;

  PROCEDURE proc_file_upload(i_user_id   IN VARCHAR2, --操作用户
                             i_file_name IN VARCHAR2, --文件名
                             i_file_data IN BLOB, --文件blob数据
                             i_ref_id    IN VARCHAR2, --文件引用编号
                             i_max_files IN VARCHAR2, --上传的最大文件数
                             o_retcode   OUT VARCHAR2,
                             o_file_id   OUT VARCHAR2);
  
  PROCEDURE proc_new_file_ref(i_user_id IN VARCHAR2,
                              o_retcode OUT VARCHAR2,
                              o_ref_id  OUT VARCHAR2);
  
  PROCEDURE proc_file_download(i_user_id   IN VARCHAR2,
                               i_file_id   IN VARCHAR2, --需要下载的文件id
                               o_retcode   OUT VARCHAR2,
                               o_file_name OUT VARCHAR2, --需要下载的文件名
                               o_file_data OUT BLOB);
  
  PROCEDURE proc_file_list(i_user_id      IN VARCHAR2,
                           i_ref_id       IN VARCHAR2, --文件引用编号
                           i_unvalid_also IN VARCHAR2, --是否只取有效数据
                           o_retcode      OUT VARCHAR2,
                           o_total_num    OUT VARCHAR2, --文件个数
                           o_file_detail  OUT ref_cur);
  
  PROCEDURE proc_file_delete(i_user_id      IN VARCHAR2,
                             i_file_ref_id  IN VARCHAR2, --文件关联编号
                             i_file_ids     IN VARCHAR2, --需要删除的的文件id序列 例如1，2，3
                             i_is_clear_all IN VARCHAR2, -- 是否删除文件关联下的所有文件
                             o_retcode      OUT VARCHAR2);
  
  PROCEDURE proc_file_updateflag(i_user_id    IN VARCHAR2,
                                 i_file_ref   IN VARCHAR2,
                                 i_file_ids   IN VARCHAR2, --需要更新的文件id序列 例如1，2，3
                                 i_flag_value IN VARCHAR2, ---标识值
                                 o_retcode    OUT VARCHAR2);
  
  PROCEDURE proc_copy_with_ref(i_user_id      IN VARCHAR2,
                               i_file_ref     IN VARCHAR2,
                               i_paste_ref_id IN VARCHAR2,
                               o_retcode      OUT VARCHAR2,
                               o_ref_id       OUT VARCHAR2);
  
  PROCEDURE proc_copy_with_file_id(i_user_id      IN VARCHAR2,
                                   i_file_ids     IN VARCHAR2,
                                   i_paste_ref_id IN VARCHAR2,
                                   o_retcode      OUT VARCHAR2,
                                   o_ref_id       OUT VARCHAR2);
  
END pckg_dams_file_upload;
//


CREATE OR REPLACE PACKAGE BODY pckg_dams_file_upload IS
  PROCEDURE proc_file_upload(i_user_id   IN VARCHAR2, --操作用户
                             i_file_name IN VARCHAR2, --文件名
                             i_file_data IN BLOB, --文件blob数据
                             i_ref_id    IN VARCHAR2, --文件引用编号
                             i_max_files IN VARCHAR2, --上传的最大文件数
                             o_retcode   OUT VARCHAR2,
                             o_file_id   OUT VARCHAR2) AS
    empty_data EXCEPTION;
    v_file_id VARCHAR2(50);
    v_msg     VARCHAR2(1000);
    v_sysdate DATE := SYSDATE;
  BEGIN
    IF i_ref_id IS NULL OR i_file_name IS NULL THEN
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;
    SELECT lpad(dams_file_id_seq.nextval, 20, '0') INTO v_file_id FROM dual;
    -- 插入文件
    INSERT INTO dams_file(file_id, file_name, file_content, create_time, create_user)
    VALUES(v_file_id, i_file_name, i_file_data, v_sysdate, i_user_id);
    -- 文件关联
    INSERT INTO dams_file_ref(file_ref_id, file_id, create_time, create_user)
    VALUES(i_ref_id, v_file_id, v_sysdate, i_user_id);
    o_file_id := v_file_id;
    o_retcode := '0';
  EXCEPTION
    WHEN empty_data THEN
      o_retcode := '-1';
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_upload;
  
  PROCEDURE proc_new_file_ref(i_user_id IN VARCHAR2,
                              o_retcode OUT VARCHAR2,
                              o_ref_id  OUT VARCHAR2) AS
  BEGIN
    --初始化处理日志的相关参数
    SELECT to_char(SYSDATE, 'YYYYMMDD') ||
           lpad(dams_file_ref_seq.nextval, 12, '0')
      INTO o_ref_id
      FROM dual;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_new_file_ref;
  
  PROCEDURE proc_file_download(i_user_id   IN VARCHAR2,
                               i_file_id   IN VARCHAR2, --需要下载的文件id
                               o_retcode   OUT VARCHAR2,
                               o_file_name OUT VARCHAR2, --需要下载的文件名
                               o_file_data OUT BLOB) AS
    v_msg VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数
    SELECT t.file_name, t.file_content
      INTO o_file_name, o_file_data
      FROM dams_file t
     WHERE t.file_id = i_file_id;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_download;
  
  PROCEDURE proc_file_list(i_user_id      IN VARCHAR2,
                           i_ref_id       IN VARCHAR2, --文件引用编号
                           i_unvalid_also IN VARCHAR2, --是否读取无效数据
                           o_retcode      OUT VARCHAR2,
                           o_total_num    OUT VARCHAR2, --文件个数
                           o_file_detail  OUT ref_cur) AS

    v_msg VARCHAR2(4000);
  BEGIN
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_file_ref r, dams_file t
     WHERE r.file_id = t.file_id
       AND r.file_ref_id = i_ref_id;

    OPEN o_file_detail FOR
      SELECT t.file_id file_id, r.file_ref_id ref_id, t.file_name file_name
        FROM dams_file_ref r, dams_file t
       WHERE r.file_id = t.file_id
         AND r.file_ref_id = i_ref_id;
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_list;
  
  PROCEDURE proc_file_delete(i_user_id      IN VARCHAR2,
                             i_file_ref_id  IN VARCHAR2, --文件关联编号
                             i_file_ids     IN VARCHAR2, --需要删除的的文件id序列 例如1，2，3
                             i_is_clear_all IN VARCHAR2, -- 是否删除文件关联下的所有文件
                             o_retcode      OUT VARCHAR2) AS
    v_msg VARCHAR2(4000);
  BEGIN
    DELETE FROM dams_file_ref r
     WHERE r.file_ref_id = i_file_ref_id
       AND r.file_id IN
           (SELECT column_value FROM TABLE(func_split2(i_file_ids)));
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_delete;
  
  PROCEDURE proc_file_updateflag(i_user_id    IN VARCHAR2,
                                 i_file_ref   IN VARCHAR2,
                                 i_file_ids   IN VARCHAR2, --需要更新的文件id序列 例如1，2，3
                                 i_flag_value IN VARCHAR2, ---标识值
                                 o_retcode    OUT VARCHAR2) IS

    v_sqlstr VARCHAR2(4000);
    v_msg    VARCHAR2(4000);
  BEGIN
    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_updateflag;
  
  PROCEDURE proc_copy_with_ref(i_user_id      IN VARCHAR2,
                               i_file_ref     IN VARCHAR2,
                               i_paste_ref_id IN VARCHAR2,
                               o_retcode      OUT VARCHAR2,
                               o_ref_id       OUT VARCHAR2) IS
    v_paste_ref_id dams_file_ref.file_ref_id%TYPE;
    new_ref_error EXCEPTION;
    v_sysdate DATE := SYSDATE;
  BEGIN
    IF i_paste_ref_id IS NULL THEN
      -- 生成ref_id
      proc_new_file_ref(i_user_id, o_retcode, v_paste_ref_id);
      IF o_retcode <> 0 THEN
        RAISE new_ref_error;
      END IF;
    ELSE
      v_paste_ref_id := i_paste_ref_id;
    END IF;
    -- 插入
    MERGE INTO dams_file_ref t
    USING (SELECT v_paste_ref_id file_ref_id, t.file_id
             FROM dams_file_ref t
            WHERE t.file_ref_id = i_file_ref) tmp
    ON (tmp.file_id = t.file_id AND tmp.file_ref_id = t.file_ref_id)
    WHEN NOT MATCHED THEN
      INSERT
        (file_ref_id, file_id, create_time, create_user)
      VALUES
        (tmp.file_ref_id, tmp.file_id, v_sysdate, i_user_id);
    o_retcode := '0';
    o_ref_id  := v_paste_ref_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_copy_with_ref;
  
  PROCEDURE proc_copy_with_file_id(i_user_id      IN VARCHAR2,
                                   i_file_ids     IN VARCHAR2,
                                   i_paste_ref_id IN VARCHAR2,
                                   o_retcode      OUT VARCHAR2,
                                   o_ref_id       OUT VARCHAR2) IS
    v_paste_ref_id dams_file_ref.file_ref_id%TYPE;
    new_ref_error EXCEPTION;
    v_sysdate DATE := SYSDATE;
  BEGIN
    IF i_paste_ref_id IS NULL THEN
      -- 生成ref_id
      proc_new_file_ref(i_user_id, o_retcode, v_paste_ref_id);
      IF o_retcode <> 0 THEN
        RAISE new_ref_error;
      END IF;
    ELSE
      v_paste_ref_id := i_paste_ref_id;
    END IF;
    -- 插入
    MERGE INTO dams_file_ref t
    USING (SELECT v_paste_ref_id file_ref_id, column_value file_id
             FROM TABLE(func_split2(i_file_ids))) tmp
    ON (tmp.file_id = t.file_id AND tmp.file_ref_id = t.file_ref_id)
    WHEN NOT MATCHED THEN
      INSERT
        (file_ref_id, file_id, create_time, create_user)
      VALUES
        (tmp.file_ref_id, tmp.file_id, v_sysdate, i_user_id);
    o_retcode := '0';
    o_ref_id  := v_paste_ref_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_copy_with_file_id;
  
END pckg_dams_file_upload;
//

CREATE OR REPLACE PACKAGE pckg_dams_dict IS
  PROCEDURE proc_gendict_query(i_table_name    IN VARCHAR2, --通用表名
                               i_typecolumn    IN VARCHAR2, --字典大类字段名
                               i_codecolumn    IN VARCHAR2, --字典小类字段名
                               i_valuecolumn   IN VARCHAR2, --字典项值字段名
                               i_langcolumn    IN VARCHAR2, --多语言字段名
                               i_filtercolumns IN VARCHAR2, --过滤字段名，分号分隔
                               i_filtervalues  IN VARCHAR2, --过滤值，分行分隔
                               o_flag          OUT VARCHAR2, --0  成功  1 取角色信息错
                               o_msg           OUT VARCHAR2, --具体错误信息
                               o_gendict       OUT SYS_REFCURSOR --通用字典表查询结果集
                               );

  PROCEDURE proc_dict_chain(i_sys_code    IN VARCHAR2, --系统编号
                            i_dict_type   IN VARCHAR2, --字典类型
                            i_parent_code IN VARCHAR2, --父字典编号
                            i_lang        IN VARCHAR2, --语言
                            o_retcode     OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                            );
END pckg_dams_dict;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_dict IS
  --查询所有的字典表
  --修改：2012-4-10  lujie   typecolumn为空则默认分类为DEFAULT
  PROCEDURE proc_gendict_query(i_table_name    IN VARCHAR2, --通用表名
                               i_typecolumn    IN VARCHAR2, --字典大类字段名
                               i_codecolumn    IN VARCHAR2, --字典小类字段名
                               i_valuecolumn   IN VARCHAR2, --字典项值字段名
                               i_langcolumn    IN VARCHAR2, --多语言字段名
                               i_filtercolumns IN VARCHAR2, --过滤字段名，分号分隔
                               i_filtervalues  IN VARCHAR2, --过滤值，分行分隔
                               o_flag          OUT VARCHAR2, --0  成功  1 取角色信息错
                               o_msg           OUT VARCHAR2, --具体错误信息
                               o_gendict       OUT SYS_REFCURSOR --通用字典表查询结果集
                               ) IS
    v_sql           VARCHAR2(2000); --存放动态SQL语句
    v_sql1          VARCHAR2(1000); --存放动态SQL语句
    v_sql2          VARCHAR2(4000); --存放过滤条件
    v_sql3          VARCHAR2(4000); --存放排序条件
    v_filtercolumns dams_arrytype;
    v_filtervalues  dams_arrytype;
    v_count         NUMBER(2);
    v_proc_name     VARCHAR2(100);
    v_inparam       VARCHAR2(4000);
    v_outparam      VARCHAR2(4000);
  BEGIN
    o_flag      := '0';
    o_msg       := '';
    v_sql       := '';
    v_sql1      := '';
    v_proc_name := 'pckg_dams_dict.proc_gendict_query';
    v_inparam   := '开始!' || '|' || i_table_name || '|' || i_typecolumn || '|' ||
                   i_codecolumn || '|' || i_valuecolumn || '|' || i_langcolumn || '|' ||
                   i_filtercolumns || '|' || i_filtervalues;
    pack_log.info(v_proc_name, '', v_inparam);
    IF i_table_name IS NOT NULL THEN
      v_sql := v_sql || '''' || i_table_name || '''';
    ELSE
      o_flag := '1';
      o_msg  := '系统启动初始化字典表失败，通用字典表表名为空!' || SQLCODE || ' ' || SQLERRM;
      RETURN;
    END IF;
    IF i_typecolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || i_typecolumn || ',';
    ELSE
      v_sql1 := v_sql1 || '''DEFAULT'',';
    END IF;
    IF i_codecolumn IS NOT NULL AND i_valuecolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || i_codecolumn || ',
             ' || i_valuecolumn;
    ELSE
      o_flag := '1';
      o_msg  := '系统启动初始化字典表失败，通用字典表字段名为空!' || SQLCODE || ' ' || SQLERRM;
      RETURN;
    END IF;
    --支持国际化
    IF i_langcolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || ',' || i_langcolumn;
    ELSE
      v_sql1 := v_sql1 || ',NULL';
    END IF;
    v_sql := 'SELECT ' || v_sql1 || ' FROM ' || REPLACE(v_sql, '''', '');
    --字典过滤
    IF i_filtercolumns IS NOT NULL AND i_filtervalues IS NOT NULL THEN
      v_filtercolumns := pckg_dams_util.func_str_to_array(i_filtercolumns, ';');
      v_filtervalues  := pckg_dams_util.func_str_to_array(i_filtervalues, ';');
      FOR i IN 1 .. v_filtercolumns.count
      LOOP
        IF i = 1 THEN
          v_sql2 := ' WHERE ';
        ELSE
          v_sql2 := v_sql2 || ' AND ';
        END IF;
        v_sql2 := v_sql2 || v_filtercolumns(i) || ' = ''' || v_filtervalues(i) || '''';
      END LOOP;
    END IF;
    SELECT COUNT(1)
      INTO v_count
      FROM user_tab_columns t
     WHERE t.table_name = upper(i_table_name)
       AND t.column_name IN ('ORDER_NO', 'DICTCODE');
    IF v_count >= 2 THEN
      v_sql3 := ' ORDER BY ORDER_NO,DICTCODE';
    ELSE
      v_sql3 := '';
    END IF;
    OPEN o_gendict FOR v_sql || v_sql2 || v_sql3;
    v_outparam := '结束!' || '|' || o_flag || '|' || o_msg;
    pack_log.info(v_proc_name, '', v_outparam);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '1';
      o_msg  := o_msg || '系统启动初始化字典表发生错误:' || SQLCODE || ' ' || SQLERRM;
  END;

  PROCEDURE proc_dict_chain(i_sys_code    IN VARCHAR2, --系统编号
                            i_dict_type   IN VARCHAR2, --字典类型
                            i_parent_code IN VARCHAR2, --父字典编号
                            i_lang        IN VARCHAR2, --语言
                            o_retcode     OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                            ) IS
    v_param     db_log.info%TYPE := i_sys_code || '|' || i_dict_type || '|' ||
                                    i_parent_code;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_DICT.PROC_DICT_CHAIN';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    o_retcode := '0';

    --查询字典项
    OPEN o_list_cur FOR
      SELECT t.dictcode, t.dictvalue
        FROM dams_gen_dict t
       WHERE t.sys_code = i_sys_code
         AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
             t.parent_code = i_parent_code)
         AND t.dicttype = i_dict_type
         AND t.valid_flag = 1
         AND t.dictlang = i_lang
       ORDER BY t.dictcode;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_dict_chain;
END pckg_dams_dict;
//

CREATE OR REPLACE PACKAGE pckg_dams_task IS
  FUNCTION func_get_task_id(i_ssic_id    IN VARCHAR2, --统一认证号
                            i_stru_id    IN VARCHAR2, --机构编码
                            i_sys_code   IN VARCHAR2, --子系统编号
                            i_biz_type   IN VARCHAR2, --申请类型
                            i_applyer    IN VARCHAR2, --申请人名字
                            i_apply_stru IN VARCHAR2, --申请部门编号
                            i_begin_date IN VARCHAR2, --查询开始时间
                            i_end_date   IN VARCHAR2, --查询结束时间
                            i_close_flag IN VARCHAR2, --是否完结，空全部，0，未完结，1，已完结
                            i_qry_type   IN VARCHAR2 --查询类型，0，个人，1，机构下辖
                            ) RETURN VARCHAR2;
  
  FUNCTION func_get_form_id_from_pdid(i_pdid        IN VARCHAR2,
                                      i_business_id IN VARCHAR2 := '')
    RETURN VARCHAR2;
  
  FUNCTION func_convert_businesstype(i_business_type IN VARCHAR2)
    RETURN VARCHAR2;
  
  FUNCTION func_is_process_dealed(i_app_id  IN VARCHAR2,
                                  i_ssic_id IN VARCHAR2) RETURN VARCHAR2;
  
  FUNCTION func_is_biz_admin(i_app_id IN VARCHAR2, i_ssic_id IN VARCHAR2)
    RETURN VARCHAR2;
  
  FUNCTION func_get_cur_dealer(i_app_id IN VARCHAR2) RETURN VARCHAR2;
  
  FUNCTION func_get_cur_task(i_app_id IN VARCHAR2) RETURN VARCHAR2;
  
  FUNCTION func_get_cur_info(i_app_id IN VARCHAR2,
                             i_type   IN VARCHAR2 := '1' --1返回处理人 2返回当前环节
                             ) RETURN VARCHAR2;
  
  PROCEDURE proc_todo_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR); --查询列表游标
  
  PROCEDURE proc_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                          i_biz_type   IN VARCHAR2, --业务类型
                          i_sys_code   IN VARCHAR2, --系统编码
                          i_close_flag IN VARCHAR2, --流程结束标志
                          i_applyer    IN VARCHAR2, --申请人
                          i_app_stru   IN VARCHAR2, --申请部门
                          i_begin_date IN VARCHAR2, --查询开始时间
                          i_end_date   IN VARCHAR2, --查询结束时间
                          i_view_type  IN VARCHAR2, --1，查询全部，2查询有权限的数据
                          i_beg_num    IN VARCHAR2, --查询开始记录
                          i_fetch_num  IN VARCHAR2, --查询记录数
                          o_retcode    OUT VARCHAR2, --返回值
                          o_total_num  OUT VARCHAR2, --记录总数
                          o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                          );
  
  PROCEDURE proc_done_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_over_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               );
  
  PROCEDURE proc_trans_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                i_biz_type  IN VARCHAR2, --业务类型
                                i_sys_code  IN VARCHAR2, --系统编码
                                i_beg_num   IN VARCHAR2, --查询开始记录
                                i_fetch_num IN VARCHAR2, --查询记录数
                                o_retcode   OUT VARCHAR2, --返回值
                                o_total_num OUT VARCHAR2, --记录总数
                                o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                );
  
  PROCEDURE proc_form_url_qry(i_form_id  IN VARCHAR2, --FORM_ID
                              i_type     IN VARCHAR2, --类型 1,审批，2，只读，3，申请
                              o_retcode  OUT VARCHAR2, --返回值
                              o_form_url OUT VARCHAR2 --form的URL
                              );
  
  PROCEDURE proc_task_dealed_qry(i_ssic_id IN VARCHAR2, --用户id
                                 i_task_id IN VARCHAR2, --taskid
                                 o_retcode OUT VARCHAR2 --返回值 0,未处理过，1，处理过
                                 );
  
  PROCEDURE proc_del_task(i_ssic_id IN VARCHAR2, --统一认证号
                          i_app_id  IN VARCHAR2, --业务类型
                          i_valid   IN VARCHAR2, --类型，0，废止，1 生效
                          o_retcode OUT VARCHAR2, --返回值
                          o_msg     OUT VARCHAR2 --返回信息
                          );
  
  PROCEDURE proc_is_process_dealed(i_ssic_id    IN VARCHAR2, --统一认证号
                                   i_app_id     IN VARCHAR2, --业务类型
                                   o_retcode    OUT VARCHAR2, --返回值
                                   o_msg        OUT VARCHAR2, --返回信息
                                   o_is_process OUT VARCHAR2);
  
  PROCEDURE proc_is_process_closed(i_app_id    IN VARCHAR2, --申请编号
                                   o_ret_code  OUT VARCHAR2, --返回值
                                   o_msg       OUT VARCHAR2,
                                   o_closeflag OUT VARCHAR2);
  
  PROCEDURE proc_is_todo_task_exist(i_ssic_id     IN VARCHAR2, --登陆人
                                    i_task_id     IN VARCHAR2, --任务ID
                                    i_business_id IN VARCHAR2, --业务主键
                                    o_retcode     OUT VARCHAR2, --返回值
                                    o_msg         OUT VARCHAR2,
                                    o_isexist     OUT VARCHAR2 --待办是否还在
                                    );
  
  PROCEDURE proc_todo_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  );
  
  PROCEDURE proc_done_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  );
   
   PROCEDURE proc_over_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  );
   
   PROCEDURE proc_trans_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                   i_biz_type  IN VARCHAR2, --业务类型
                                   i_sys_code  IN VARCHAR2, --系统编码
                                   i_beg_num   IN VARCHAR2, --查询开始记录
                                   i_fetch_num IN VARCHAR2, --查询记录数
                                   o_retcode   OUT VARCHAR2, --返回值
                                   o_total_num OUT VARCHAR2, --记录总数
                                   o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                   );
   
   PROCEDURE proc_trash_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                           i_biz_type   IN VARCHAR2, --业务类型
                           i_sys_code   IN VARCHAR2, --系统编码
                           i_condition  IN VARCHAR2, --系统编号或事项
                           i_start_date IN VARCHAR2, --开始时间
                           i_end_date   IN VARCHAR2, --结束时间
                           i_beg_num    IN VARCHAR2, --查询开始记录
                           i_fetch_num  IN VARCHAR2, --查询记录数
                           o_retcode    OUT VARCHAR2, --返回值
                           o_total_num  OUT VARCHAR2, --记录总数
                           o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                           );
   
   PROCEDURE proc_se_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_apply_stru IN VARCHAR2, --申请部门
                             i_reason     IN VARCHAR2, --材料名称与份数
                             i_sec_lv     IN VARCHAR2, --密级
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_sf_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_apply_stru IN VARCHAR2, --申请部门
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --证卡使用人
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_sn_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_dev_type   IN VARCHAR2, --设备类型
                             i_close_flag IN VARCHAR2, --流转状态
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_so_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_app_id     IN VARCHAR2, --核实单号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_office_id  IN VARCHAR2, --使用人处室
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_sp_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_app_id     IN VARCHAR2, --核实单号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_office_id  IN VARCHAR2, --使用人处室
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_st_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, --业务类型
                             i_sys_code   IN VARCHAR2, --系统编码
                             i_applyer    IN VARCHAR2, --申请人
                             i_begin_date IN VARCHAR2, --查询开始时间
                             i_end_date   IN VARCHAR2, --查询结束时间
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_sx_task_qry(i_ssic_id       IN VARCHAR2, --统一认证号
                             i_biz_type      IN VARCHAR2, --业务类型
                             i_sys_code      IN VARCHAR2, --系统编码
                             i_close_flag    IN VARCHAR2, --流程结束标志
                             i_applyer       IN VARCHAR2, --申请人
                             i_begin_date    IN VARCHAR2, --查询开始时间
                             i_end_date      IN VARCHAR2, --查询结束时间
                             i_beg_num       IN VARCHAR2, --查询开始记录
                             i_fetch_num     IN VARCHAR2, --查询记录数
                             i_city          IN VARCHAR2,
                             i_origin_app_id IN VARCHAR2,
                             o_retcode       OUT VARCHAR2, --返回值
                             o_total_num     OUT VARCHAR2, --记录总数
                             o_list_cur      OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_ab_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, --业务类型
                             i_sys_code   IN VARCHAR2, --系统编码
                             i_close_flag IN VARCHAR2, --流程结束标志
                             i_applyer    IN VARCHAR2, --申请人
                             i_app_stru   IN VARCHAR2, --申请部门
                             i_begin_date IN VARCHAR2, --查询开始时间
                             i_end_date   IN VARCHAR2, --查询结束时间
                             i_view_type  IN VARCHAR2, --1，查询全部，2查询有权限的数据
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             );
   
   PROCEDURE proc_get_latest_draft_task(i_ssic_id IN VARCHAR2, --统一认证号
                                       o_retcode OUT VARCHAR2, --返回值
                                       o_msg     OUT VARCHAR2,
                                       o_detail  OUT SYS_REFCURSOR --查询列表游标
                                       );
   
   PROCEDURE proc_inner_task_finish(i_app_id      IN VARCHAR2,
                                   i_business_id IN VARCHAR2,
                                   i_ssic_id     IN VARCHAR2,
                                   i_sys_code    IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2 --返回值
                                   );
   
   --PROCEDURE proc_inner_task_finish(i_app_id      IN VARCHAR2,
   --                                i_business_id IN VARCHAR2,
   --                                i_ssic_id     IN VARCHAR2, --可为空
   --                                i_sys_code    IN VARCHAR2,
   --                                i_url         IN VARCHAR2, --更新url
   --                                o_retcode     OUT VARCHAR2 --返回值
   --                                );
   
   PROCEDURE proc_qry_inner_todo_url(i_ssic_id     IN VARCHAR2, --统一认证号
                                    i_task_id     IN VARCHAR2, --业务类型
                                    i_business_id IN VARCHAR2, --系统编码
                                    i_sys_code    IN VARCHAR2,
                                    o_ret_code    OUT VARCHAR2, --返回值
                                    o_msg         OUT VARCHAR2,
                                    o_url         OUT VARCHAR2 --记录总数
                                    );
   
   PROCEDURE proc_inner_task_finish_with_jz(i_app_id      IN VARCHAR2,
                                           i_business_id IN VARCHAR2,
                                           i_ssic_id     IN VARCHAR2, --可为空
                                           i_sys_code    IN VARCHAR2,
                                           i_url         IN VARCHAR2, --更新url
                                           o_retcode     OUT VARCHAR2 --返回值
                                           );
   
   PROCEDURE proc_qry_todo_task_num(i_ssic_id     IN VARCHAR2, --统一认证号
                                   i_biz_type    IN VARCHAR2, --业务类型
                                   i_sys_code    IN VARCHAR2, --系统编码
                                   i_business_id IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2, --返回值
                                   o_total_num   OUT VARCHAR2 --记录总数
                                   );
   
   --PROCEDURE proc_todo_task_ac_qry(i_ssic_id   IN VARCHAR2, --统一认证号
   --                               i_biz_type  IN VARCHAR2, --业务类型
   --                               i_sys_code  IN VARCHAR2, --系统编码
   --                               i_status    IN VARCHAR2, --状态 空全部 1超期
   --                               i_beg_num   IN VARCHAR2, --查询开始记录
   --                               i_fetch_num IN VARCHAR2, --查询记录数
   --                               o_retcode   OUT VARCHAR2, --返回值
   --                               o_total_num OUT VARCHAR2, --记录总数
   --                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
   --                               );
   
   PROCEDURE proc_done_task_ac_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_status    IN VARCHAR2, --状态 1-承会办部门已接收 2-承办部门内部未处理 3-承办部门已超期
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  );
   
   --PROCEDURE proc_task_qry_ac(i_ssic_id           IN VARCHAR2, --统一认证号
   --                          i_branch_id         IN VARCHAR2, --机构号
   --                          i_biz_type          IN VARCHAR2, --业务类型
   --                          i_sys_code          IN VARCHAR2, --系统编码
   --                          i_close_flag        IN VARCHAR2, --流程结束标志
   --                          i_applyer           IN VARCHAR2, --申请人
   --                          i_begin_date        IN VARCHAR2, --查询开始时间
   --                          i_end_date          IN VARCHAR2, --查询结束时间
   --                          i_serial_num        IN VARCHAR2, --督办编号
   --                          i_sup_title         IN VARCHAR2, --督办标题
   --                          i_sup_basis         IN VARCHAR2, --督办依据
   --                          i_cur_status        IN VARCHAR2, --当前状态
   --                          i_cb_stru_name      IN VARCHAR2, --承办单位
   --                          i_hb_stru_name      IN VARCHAR2, --会办单位
   --                          i_cb_stru_comment   IN VARCHAR2, --承办单位意见
   --                          i_hb_stru_comment   IN VARCHAR2, --会办单位意见
   --                          i_leader_comment    IN VARCHAR2, --领导意见
   --                          i_is_adopted        IN VARCHAR2, --是否采纳
   --                          i_is_overtime       IN VARCHAR2, --是否超时
   --                          i_overtime_days_min IN VARCHAR2, --超时天数最小值
   --                          i_overtime_days_max IN VARCHAR2, --超时天数最大值
   --                          i_beg_num           IN VARCHAR2, --查询开始记录
   --                          i_fetch_num         IN VARCHAR2, --查询记录数
   --                          o_retcode           OUT VARCHAR2, --返回值
   --                          o_total_num         OUT VARCHAR2, --记录总数
   --                          o_list_cur          OUT SYS_REFCURSOR --查询列表游标
   --                          );
   
END pckg_dams_task;
//


CREATE OR REPLACE PACKAGE BODY pckg_dams_task IS
  FUNCTION func_get_task_id(i_ssic_id    IN VARCHAR2, --统一认证号
                            i_stru_id    IN VARCHAR2, --机构编码
                            i_sys_code   IN VARCHAR2, --子系统编号
                            i_biz_type   IN VARCHAR2, --申请类型
                            i_applyer    IN VARCHAR2, --申请人名字
                            i_apply_stru IN VARCHAR2, --申请部门编号
                            i_begin_date IN VARCHAR2, --查询开始时间
                            i_end_date   IN VARCHAR2, --查询结束时间
                            i_close_flag IN VARCHAR2, --是否完结，空全部，0，未完结，1，已完结
                            i_qry_type   IN VARCHAR2 --查询类型，0，个人，1，机构下辖
                            ) RETURN VARCHAR2 IS
    v_sql   VARCHAR2(32000);
    v_where VARCHAR2(32000);
  BEGIN
    IF i_qry_type = 0 THEN
      -- 个人
      v_where := 'EXISTS (SELECT 1 FROM   viw_dams_wf_all_task tt1 WHERE  tt1.BUSINESS_ID = t1.business_id AND tt1.SSIC_ID = ''' || i_ssic_id || ''')';
    ELSIF i_qry_type = 1 THEN
      -- 机构下辖
      v_where := '(EXISTS (SELECT 1 FROM DAMS_BRANCH_RELATION tt1 WHERE INSTR(''' || i_stru_id || ''' ,tt1.UP_BNCH_ID) > 0 AND tt1.bnch_id = t1.creator_stru ) OR EXISTS (SELECT 1 FROM   viw_dams_wf_all_task tt1 WHERE  tt1.BUSINESS_ID = t1.business_id AND tt1.SSIC_ID = ''' || i_ssic_id || '''))';
    END IF;
    IF i_sys_code IS NOT NULL THEN
      v_where := v_where || ' AND t1.sys_code = ''' || i_sys_code || ''' ';
    END IF;
    IF i_biz_type IS NOT NULL THEN
      v_where := v_where || ' AND t1.business_type = ''' || i_biz_type || ''' ';
    END IF;
    IF i_applyer IS NOT NULL THEN
      v_where := v_where || ' AND exists (select 1 from dams_user u where t1.creator_id = u.ssic_id and u.name like  ''%' || i_applyer || '%'') ';
    END IF;
    IF i_apply_stru IS NOT NULL THEN
      v_where := v_where || ' AND exists (select 1 from dams_branch_relation rel where rel.bnch_id = t1.creator_stru and rel.up_bnch_id = ''' || i_apply_stru || ''' ) ';
    END IF;
    IF i_close_flag IS NOT NULL THEN
      v_where := v_where || ' AND t1.close_flag = ''' || i_close_flag || ''' ';
    END IF;
    IF i_begin_date IS NOT NULL THEN
      v_where := v_where || ' AND t1.start_time >= to_date(''' || i_begin_date || ''',''YYYY-MM-DD'') ';
    END IF;
    IF i_end_date IS NOT NULL THEN
      v_where := v_where || ' AND t1.start_time < (to_date(''' || i_end_date || ''',''YYYY-MM-DD'') + 1) ';
    END IF;
    --查询task_id
    v_where := v_where || ' AND t2.business_id = t1.business_id AND t2.create_time = (SELECT MAX(create_time) FROM viw_dams_wf_all_task tt2 WHERE tt2.business_id = t2.business_id)';

    v_sql := 'insert into dams_tmp_task_id (business_id,task_id,business_type,sys_code,start_time,complete_time,title,creator_id,creator_stru) select t1.business_id,t2.task_id,t1.business_type,t1.sys_code,t1.start_time,t1.complete_time,t1.title,t1.creator_id,t1.creator_stru from viw_dams_wf_all_process t1,viw_dams_wf_all_task t2 where ' || v_where;
    --插入符合条件的业务主键
    EXECUTE IMMEDIATE v_sql;
    --更新最新的task_id和任务信息
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN - 1;
  END func_get_task_id;
  
  FUNCTION func_get_form_id_from_pdid(i_pdid        IN VARCHAR2,
                                      i_business_id IN VARCHAR2 := '')
    RETURN VARCHAR2 IS
    v_rlt dams_form.form_id%TYPE;
  BEGIN
    --20141013修改
    SELECT MAX(s.form_id)
      INTO v_rlt
      FROM dams_save_info s
     WHERE s.app_id = i_business_id;
    IF v_rlt IS NULL THEN
      SELECT MAX(t.form_id)
        INTO v_rlt
        FROM dams_wf_process_form t
       WHERE t.pdid = i_pdid;
      IF v_rlt IS NULL THEN
        RETURN i_pdid;
      END IF;
    END IF;
    RETURN v_rlt;
  END func_get_form_id_from_pdid;
  
  FUNCTION func_convert_businesstype(i_business_type IN VARCHAR2)
    RETURN VARCHAR2 IS
    v_business_type ois_gen_dict.dictcode%TYPE;
  BEGIN
    IF i_business_type IN ('RE001', 'RE002', 'RE003', 'RE004', 'RE005') THEN
      v_business_type := 'RE01'; --离职审查
    ELSIF i_business_type IN ('RE011',
                              'RE012',
                              'RE013',
                              'RE014',
                              'RE015',
                              'RE016',
                              'RE017',
                              'RE021',
                              'RE022',
                              'RE023',
                              'RE024',
                              'RE025',
                              'RE026') THEN
      v_business_type := 'RE02'; --；离职手续办理
    ELSE
      v_business_type := i_business_type;
    END IF;
    RETURN v_business_type;
  END func_convert_businesstype;
  
  FUNCTION func_is_process_dealed(i_app_id  IN VARCHAR2,
                                  i_ssic_id IN VARCHAR2) RETURN VARCHAR2 IS
    v_count NUMBER;
  BEGIN
    --判断是否经手过
    SELECT COUNT(1)
      INTO v_count
      FROM viw_dams_wf_all_task t2
     WHERE t2.business_id = i_app_id
       AND t2.ssic_id = i_ssic_id
       AND t2.task_state IN ('completed');
    --判断是否转授权处理
    SELECT COUNT(1) + v_count
      INTO v_count
      FROM viw_dams_wf_all_task t2
     WHERE t2.business_id = i_app_id
       AND t2.auth_state = 1
       AND t2.ssic_id_tran = i_ssic_id
       AND t2.task_state IN ('completed');
    --判断是否待办
    SELECT COUNT(1) + v_count
      INTO v_count
      FROM dams_wf_task t1
     WHERE t1.business_id = i_app_id
       AND t1.task_state IN ('open', 'suspended')
       AND EXISTS (SELECT '1'
              FROM jbpm4_participation t2
             WHERE t2.userid_ = i_ssic_id
               AND t2.task_ = t1.task_id);
    --判断是否是竞争环节的处理人
    SELECT COUNT(1) + v_count
      INTO v_count
      FROM dams_wf_participation p2
     WHERE p2.business_id = i_app_id
       AND p2.ssic_id = i_ssic_id;
    --返回
    IF v_count > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_is_process_dealed;
  
  FUNCTION func_is_biz_admin(i_app_id IN VARCHAR2, i_ssic_id IN VARCHAR2)
    RETURN VARCHAR2 IS
    v_count    NUMBER;
    v_biz_type VARCHAR2(30);
  BEGIN
    --获取业务类型
    SELECT t.business_type
      INTO v_biz_type
      FROM viw_dams_wf_all_process t
     WHERE t.business_id = i_app_id;
    SELECT COUNT(1)
      INTO v_count
      FROM dams_user_role_rel t
     WHERE EXISTS (SELECT 1
              FROM dams_prm_biz_admin t1
             WHERE t1.biz_type = v_biz_type
               AND t1.role = t.role_id)
       AND t.ssic_id = i_ssic_id;
    --返回
    IF v_count > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_is_biz_admin;
  
  FUNCTION func_get_cur_dealer(i_app_id IN VARCHAR2) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
    SELECT func_get_cur_info(i_app_id, '1') INTO v_rlt FROM dual;
    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_get_cur_dealer;
  
  FUNCTION func_get_cur_task(i_app_id IN VARCHAR2) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
    SELECT func_get_cur_info(i_app_id, '2') INTO v_rlt FROM dual;
    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_get_cur_task;
  
  FUNCTION func_get_cur_info(i_app_id IN VARCHAR2,
                             i_type   IN VARCHAR2 := '1' --1返回处理人 2返回当前环节
                             ) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
    --逻辑同pckg_dams_jbpm.proc_jbpm_traces_curinfo
    --重写了逻辑，支持平台子流程的情况 moded by xiesy 9.29
    SELECT decode(i_type,
                  '1',
                  nvl(tmp.name, '无'),
                  '2',
                  nvl(decode(tmp.cnt,
                             1,
                             tmp.latest_task_name,
                             (SELECT MAX(x.outgoing_name) keep(dense_rank LAST ORDER BY x.create_time)
                                FROM dams_wf_task x
                               WHERE x.business_id = tmp.latest_business)),
                      '无'),
                  '')
      INTO v_rlt
      FROM (SELECT COUNT(DISTINCT t.business_id) cnt,
                   MAX(sp.source_business_id) keep(dense_rank LAST ORDER BY sp.lv) latest_business,
                   MAX(t.task_name) keep(dense_rank LAST ORDER BY t.create_time) latest_task_name,
                   listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
              FROM dams_wf_task t,
                   jbpm4_participation p,
                   dams_user u1,
                   (SELECT r.business_id, LEVEL lv, r.source_business_id
                      FROM dams_wf_sub_process_rel r
                     START WITH r.source_business_id = i_app_id
                    CONNECT BY PRIOR r.business_id = r.source_business_id
                    UNION ALL
                    SELECT i_app_id, 0 lv, ''
                      FROM dual) sp
             WHERE t.business_id = sp.business_id
               AND t.task_state = 'open'
               AND t.task_id = p.task_
               AND p.userid_ = u1.ssic_id(+)) tmp;
    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_get_cur_info;
  
  PROCEDURE proc_todo_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                      i_sys_code || '|' || i_beg_num || '|' ||
                                      i_fetch_num;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TODO_TASK_QRY';
    v_step        db_log.step_no%TYPE := 0;
    v_sav_num     NUMBER;
    v_out_num     NUMBER; --kfzx-guoq03 add;
    v_inner_num   NUMBER; --非流程待办数量
    v_person_name dams_user.name%TYPE;
    v_is_zjl VARCHAR2(1); --20150821 ggc 总经理标题去掉’草稿‘
  BEGIN
    pack_log.info(v_proc_name,pack_log.start_step,pack_log.start_msg || v_param);
	o_retcode := '0';
    SELECT COUNT(1)
      INTO v_is_zjl
      FROM dams_user_role_rel t
     WHERE t.ssic_id = i_ssic_id
       AND t.sys_code = 'GW'
       AND t.role_id = 'DR00000004'
       AND t.auth_state IN ('0', '1', '2');
    SELECT MAX(t.name)
      INTO v_person_name
      FROM dams_user t
     WHERE t.ssic_id = i_ssic_id;
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_task t1, dams_wf_process t4
     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
       AND EXISTS (SELECT '1'
              FROM jbpm4_participation t2
             WHERE t2.userid_ = i_ssic_id
               AND t2.task_ = t1.task_id)
       AND t1.procinst_id = t4.procinst_id
          --AND T1.IS_FIRST = 1
       AND t1.task_state IN ('open', 'suspended')
       AND nvl(t4.business_type, 0) =
           nvl(i_biz_type, nvl(t4.business_type, 0))
       AND t4.valid_flag = '1';
    --草稿数量
    SELECT COUNT(1)
      INTO v_sav_num
      FROM dams_save_info t,
           (SELECT t2.pdid, MAX(t2.sys_code) sys_code
              FROM dams_wf_process_conf t2
             GROUP BY t2.pdid) t1
     WHERE t.applyer = i_ssic_id
       AND t1.pdid = t.pdid
       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
       AND nvl(t.biz_type, 0) = nvl(i_biz_type, nvl(t.biz_type, 0))
       AND t.valid_flag = 1;
    --非流程待办
    SELECT COUNT(1)
      INTO v_inner_num
      FROM dams_inner_todo_task t
     WHERE t.ssic_id = i_ssic_id
       AND t.sys_code = nvl(i_sys_code, t.sys_code)
       AND nvl(t.business_type, 0) =
           nvl(i_biz_type, nvl(t.business_type, 0))
       AND t.task_state = 'open';
    --kfzx-guoq03 add start 外部待办
    SELECT COUNT(1)
      INTO v_out_num
      FROM dams_outer_todo_task t
     WHERE t.ssic_id = i_ssic_id
       AND t.sys_code = nvl(i_sys_code, t.sys_code)
       AND t.task_state = 'open'
       AND nvl(func_convert_businesstype(t.business_type), 0) =
           nvl(i_biz_type,
               nvl(func_convert_businesstype(t.business_type), 0));
    --kfzx-guoq03 add end 外部待办
    --kfzx-guoq03 modified O_TOTAL_NUM := O_TOTAL_NUM + V_SAV_NUM;
    o_total_num := o_total_num + v_inner_num + v_sav_num + v_out_num;
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             role_id,
             prev_task_id,
             prev_task_name,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stur_name,
             cfg_id,
             url, --kfzx-guoq03 add
             show_business_id,
             rn
        FROM (SELECT task_id,
                     task_name,
                     user_name,
                     stru_id,
                     role_id,
                     prev_task_id,
                     prev_task_name,
                     prev_ssic_id,
                     prev_person_name,
                     prev_stru_id,
                     procinst_id,
                     process_id,
                     process_name,
                     form_id,
                     func_convert_businesstype(business_type) business_type,
                     business_id,
                     title,
                     create_time,
                     take_time,
                     end_time,
                     task_state,
                     outgoing_name,
                     opinion,
                     apply_name,
                     apply_time,
                     app_stru,
                     app_stur_name,
                     cfg_id,
                     url,
                     coalesce(out_sys_seq, business_id) show_business_id,
                     row_number() over(ORDER BY tn.ordertime DESC) rn
                FROM (SELECT '0' task_id,
                             '拟稿' task_name,
                             t.applyer,
                             v_person_name user_name,
                             t.app_stru stru_id,
                             '' role_id,
                             0 prev_task_id,
                             '拟稿' prev_task_name,
                             t.applyer prev_ssic_id,
                             v_person_name prev_person_name,
                             '' prev_stru_id,
                             '' prev_role_id,
                             t.app_id procinst_id,
                             '' process_id,
                             '' process_name,
                             func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
                             t.biz_type business_type,
                             t.app_id business_id,
                             t.info title,
                             to_char(t.app_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
                             to_char(t.app_time, 'YYYY-MM-DD') take_time,
                             '' end_time,
                             'draft' task_state,
                             '' outgoing_name,
                             '' opinion,
                             t.applyer applyer,
                             v_person_name apply_name,
                             to_char(t.app_time, 'YYYY-MM-DD') apply_time,
                             t.app_stru,
                             (SELECT b.stru_sname
                                FROM dams_branch b
                               WHERE b.stru_id = t.app_stru) app_stur_name,
                             NULL cfg_id,
                             '' url,
                             to_char(t.app_time, 'yyyymmddhh24miss') ordertime,
                             '' out_sys_seq
                      --kfzx-guoq03 add ,row_number() over(ORDER BY t.app_time DESC) rn
                        FROM dams_save_info t,
                             (SELECT t2.pdid, MAX(t2.sys_code) sys_code
                                FROM dams_wf_process_conf t2
                               GROUP BY t2.pdid) t1
                       WHERE t.applyer = i_ssic_id
                         AND t1.pdid = t.pdid
                         AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                         AND nvl(t.biz_type, 0) =
                             nvl(i_biz_type, nvl(t.biz_type, 0))
                         AND t.valid_flag = 1
                      UNION ALL (SELECT to_char(t1.task_id),
                                       t1.task_name,
                                       t1.ssic_id,
                                       v_person_name user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       (SELECT wt.task_name
                                          FROM dams_wf_task wt
                                         WHERE wt.task_id = t1.prev_task_id) prev_task_name,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       to_char(t1.create_time,
                                               'YYYY-MM-DD HH24:MI:SS') create_time,
                                       to_char(t1.take_time, 'YYYY-MM-DD') take_time,
                                       to_char(t1.end_time, 'YYYY-MM-DD') end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       t4.cfg_id cfgid,
                                       '' url,
                                       to_char(t1.create_time,
                                               'yyyymmddhh24miss') ordertime,
                                       '' out_sys_seq
                                --kfzx-guoq03 add , row_number() over(ORDER BY t1.create_time DESC) + v_sav_num rn
                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                                 WHERE t4.sys_code =
                                       nvl(i_sys_code, t4.sys_code)
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM jbpm4_participation t2
                                         WHERE t2.userid_ = i_ssic_id
                                           AND t2.task_ = t1.task_id)
                                   AND t1.procinst_id = t4.procinst_id
                                      --AND T1.IS_FIRST = 1
                                   AND t1.task_state IN ('open', 'suspended')
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1')
                      --非流程待办
                      UNION ALL (SELECT i.task_id,
                                       '' task_name,
                                       i.ssic_id,
                                       v_person_name user_name,
                                       '' stru_id,
                                       'INNER' role_id, --非流程待办
                                       NULL prev_task_id,
                                       i.prev_task_name prev_task_name,
                                       i.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = i.prev_ssic_id) prev_person_name,
                                       '' prev_stru_id,
                                       '' prev_role_id,
                                       NULL procinst_id,
                                       NULL process_id,
                                       '' process_name,
                                       NULL form_id,
                                       i.business_type,
                                       i.business_id,
                                       i.title,
                                       to_char(i.create_time,
                                               'YYYY-MM-DD HH24:MI:SS') create_time,
                                       to_char(i.take_time, 'yyyy-mm-dd') take_time,
                                       '' end_time,
                                       i.task_state,
                                       '' outgoing_name,
                                       '' opinion,
                                       i.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = i.creator_id) apply_name,
                                       '' apply_time,
                                       i.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = i.creator_stru) app_stru_name,
                                       NULL cfgid,
                                       i.url,
                                       to_char(i.create_time,
                                               'yyyymmddhh24miss') ordertime,
                                       i.in_sys_seq
                                  FROM dams_inner_todo_task i
                                 WHERE i.ssic_id = i_ssic_id
                                   AND i.task_state = 'open'
                                   AND i.sys_code =
                                       nvl(i_sys_code, i.sys_code)
                                   AND nvl(i.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(i.business_type, 0)))
                      --kfzx-guoq03 added start
                      UNION ALL (SELECT to_char(outtask.task_id),
                                       outtask.task_name,
                                       outtask.ssic_id,
                                       v_person_name user_name,
                                       outtask.stru_id,
                                       'OUTER' role_id, --判断是否外部标志
                                       outtask.prev_task_id,
                                       outtask.prev_task_name,
                                       outtask.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id =
                                               outtask.prev_ssic_id) prev_person_name,
                                       outtask.prev_stru_id,
                                       outtask.prev_role_id,
                                       outtask.procinst_id,
                                       outtask.process_id,
                                       outtask.process_name,
                                       outtask.form_id,
                                       outtask.business_type,
                                       outtask.business_id,
                                       decode(v_is_zjl,
                                              '0',
                                              outtask.title,
                                              REPLACE(outtask.title,
                                                      '[草稿]',
                                                      '')), --20151110 ggc 总经理标题去掉’草稿‘
                                       to_char(to_date(outtask.create_time,
                                                       'yyyymmddhh24miss'),
                                               'YYYY-MM-DD HH24:MI:SS') create_time,
                                       to_char(to_date(outtask.take_time,
                                                       'yyyymmddhh24miss'),
                                               'yyyy-mm-dd') take_time,
                                       to_char(to_date(outtask.end_time,
                                                       'yyyymmddhh24miss'),
                                               'yyyy-mm-dd') end_time,
                                       outtask.task_state,
                                       outtask.outgoing_name,
                                       outtask.opinion,
                                       outtask.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = outtask.creator_id) apply_name,
                                       to_char(to_date(outtask.start_time,
                                                       'yyyymmddhh24miss'),
                                               'yyyy-mm-dd') apply_time,
                                       outtask.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id =
                                               outtask.creator_stru) app_stru_name,
                                       outtask.cfgid,
                                       t.sysurl || '/' || t.context || '/' ||
                                       outtask.url url,
                                       outtask.create_time ordertime,
                                       decode(outtask.task_id,
                                              '0',
                                              decode(v_is_zjl,
                                                     '0',
                                                     outtask.out_sys_seq,
                                                     decode(outtask.sys_code,
                                                            'GW',
                                                            decode(outtask.business_type,
                                                                   '003',
                                                                   '行收文',
                                                                   '004',
                                                                   '部收文',
                                                                   outtask.out_sys_seq),
                                                            outtask.out_sys_seq)),
                                              outtask.out_sys_seq) out_sys_seq
                                --kfzx-guoq03 ,row_number() over(ORDER BY outtask.create_time DESC) + v_inner_num rn
                                  FROM dams_outer_todo_task outtask,
                                       dams_sys_url_cfg     t
                                 WHERE outtask.sys_code =
                                       nvl(i_sys_code, outtask.sys_code)
                                   AND nvl(func_convert_businesstype(outtask.business_type),
                                           0) = nvl(i_biz_type,
                                                    nvl(func_convert_businesstype(outtask.business_type),
                                                        0))
                                   AND outtask.ssic_id = i_ssic_id
                                   AND outtask.sys_code = t.sys_code
                                   AND outtask.task_state = 'open')
                      --kfzx-guoq03 added end
                      ) tn)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name,pack_log.end_step,pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_todo_task_qry;
  
  PROCEDURE proc_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                          i_biz_type   IN VARCHAR2, --业务类型
                          i_sys_code   IN VARCHAR2, --系统编码
                          i_close_flag IN VARCHAR2, --流程结束标志
                          i_applyer    IN VARCHAR2, --申请人
                          i_app_stru   IN VARCHAR2, --申请部门
                          i_begin_date IN VARCHAR2, --查询开始时间
                          i_end_date   IN VARCHAR2, --查询结束时间
                          i_view_type  IN VARCHAR2, --1，查询全部，2查询有权限的数据
                          i_beg_num    IN VARCHAR2, --查询开始记录
                          i_fetch_num  IN VARCHAR2, --查询记录数
                          o_retcode    OUT VARCHAR2, --返回值
                          o_total_num  OUT VARCHAR2, --记录总数
                          o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                          ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                      i_sys_code || '|' || i_close_flag || '|' ||
                                      i_applyer || '|' || i_app_stru || '|' ||
                                      i_begin_date || '|' || i_end_date || '|' ||
                                      i_beg_num || '|' || i_fetch_num;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK_BAK.PROC_TASK_QRY';
    v_step        db_log.step_no%TYPE := 0;
    v_min_date    VARCHAR2(10) := '1900-01-01';
    v_end_date    DATE := SYSDATE;
    v_num         NUMBER;
    v_status_code VARCHAR2(2) := '1';
    v_qxsq_stru   VARCHAR2(1000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_end_date IS NULL OR i_end_date = '' THEN
      NULL;
    ELSE
      v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
    END IF;
    --SELECT dams_conn(bc.major_stru_id)
    SELECT bc.major_stru_id
      INTO v_qxsq_stru
      FROM dams_user_role_rel t, dams_branch_catalog bc
     WHERE t.ssic_id = i_ssic_id
       AND t.role_id = 'DR20000029'
       AND t.auth_state IN ('0', '1')
       AND t.stru_id = bc.stru_id;

    IF (i_sys_code IS NOT NULL OR i_sys_code <> '') AND
       (i_biz_type IS NOT NULL OR i_biz_type <> '') THEN
      --sys_code ,biz_type 都不空
      SELECT COUNT(1)
        INTO v_num
        FROM dams_user_role_rel t1
       WHERE EXISTS (SELECT '1'
                FROM dams_prm_biz_admin t2
               WHERE t2.biz_type = i_biz_type
                 AND t2.role = t1.role_id)
         AND t1.ssic_id = i_ssic_id;
      IF v_num > 0 THEN
        --是biz_type ADMIN
        v_status_code := '2';
      ELSE
        v_status_code := '1';
      END IF;
    ELSIF (i_sys_code IS NOT NULL OR i_sys_code <> '') AND
          (i_biz_type IS NULL OR i_biz_type = '') THEN
      --sys_code不空 ,biz_type 空
      SELECT COUNT(1)
        INTO v_num
        FROM dams_user_role_rel t1
       WHERE EXISTS (SELECT '1'
                FROM dams_prm_biz_admin t2
               WHERE t2.biz_type IN
                     (SELECT t3.dictcode
                        FROM dams_gen_dict t3
                       WHERE t3.dicttype = 'BIZ_TYPE'
                         AND t3.parent_code = i_sys_code
                         AND t3.sys_code = 'DAMS'
                         AND t3.dictlang = 'zh_CN')
                 AND t2.role = t1.role_id)
         AND t1.ssic_id = i_ssic_id;
      IF v_num > 0 THEN
        --是biz_type ADMIN
        v_status_code := '3';
      ELSE
        v_status_code := '1';
      END IF;
    ELSIF (i_sys_code IS NULL OR i_sys_code = '') AND
          (i_biz_type IS NULL OR i_biz_type = '') THEN
      --sys_code空 ,biz_type 空
      SELECT COUNT(1)
        INTO v_num
        FROM dams_user_role_rel t1
       WHERE EXISTS (SELECT '1'
                FROM dams_prm_biz_admin t2
               WHERE t2.biz_type IN
                     (SELECT t3.dictcode
                        FROM dams_gen_dict t3
                       WHERE t3.dicttype = 'BIZ_TYPE'
                         AND t3.sys_code = 'DAMS'
                         AND t3.dictlang = 'zh_CN')
                 AND t2.role = t1.role_id)
         AND t1.ssic_id = i_ssic_id;
      IF v_num > 0 THEN
        --是biz_type ADMIN
        v_status_code := '4';
      ELSE
        v_status_code := '1';
      END IF;
    END IF;
    IF v_status_code = '4' THEN
      --sys_code空 ,biz_type 空
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT 1
                FROM (SELECT t3.sys_code, t1.stru_id
                        FROM viw_dams_wf_all_task    t1,
                             viw_dams_wf_all_process t3,
                             dams_user               t4
                       WHERE t1.procinst_id = t3.procinst_id
                            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                         AND EXISTS (SELECT '1'
                                FROM dams_wf_process_conf t4
                               WHERE t4.pdid = t3.process_id
                                 AND t4.sys_code =
                                     nvl(i_sys_code, t3.sys_code))
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_task t2
                               WHERE t2.procinst_id = t3.procinst_id
                                 AND t2.ssic_id = i_ssic_id
                                 AND t2.task_state IN ('completed'))
                         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                            --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                         AND t3.creator_id = t4.ssic_id
                            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                         AND t4.name = nvl(i_applyer, t4.name)
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_process t5
                               WHERE t5.procinst_id = t3.procinst_id
                                 AND t5.start_time >=
                                     to_date(nvl(i_begin_date, v_min_date),
                                             'YYYY-MM-DD')
                                 AND t5.start_time <= v_end_date)
                         AND t1.create_time =
                             (SELECT MAX(tt1.create_time)
                                FROM viw_dams_wf_all_task tt1
                               WHERE tt1.procinst_id = t3.procinst_id)
                      UNION ALL
                      SELECT t3.sys_code, t1.stru_id
                        FROM viw_dams_wf_all_task    t1,
                             viw_dams_wf_all_process t3,
                             dams_user               t4
                       WHERE t1.procinst_id = t3.procinst_id
                            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                         AND EXISTS (SELECT '1'
                                FROM dams_wf_process_conf t4
                               WHERE t4.pdid = t3.process_id
                                 AND t4.sys_code =
                                     nvl(i_sys_code, t3.sys_code))
                         AND NOT EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_task t2
                               WHERE t2.procinst_id = t3.procinst_id
                                 AND t2.ssic_id = i_ssic_id
                                 AND t2.task_state IN ('completed'))
                         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                            --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                         AND t3.business_type IN
                             (SELECT DISTINCT t1.biz_type
                                FROM dams_prm_biz_admin t1,
                                     dams_user_role_rel t2
                               WHERE t1.biz_type IN
                                     (SELECT t3.dictcode
                                        FROM dams_gen_dict t3
                                       WHERE t3.dicttype = 'BIZ_TYPE'
                                         AND t3.sys_code = 'DAMS'
                                         AND t3.dictlang = 'zh_CN')
                                 AND t1.role = t2.role_id
                                 AND t2.ssic_id = i_ssic_id)

                         AND t3.creator_id = t4.ssic_id
                            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                         AND t4.name = nvl(i_applyer, t4.name)
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_process t5
                               WHERE t5.procinst_id = t3.procinst_id
                                 AND t5.start_time >=
                                     to_date(nvl(i_begin_date, v_min_date),
                                             'YYYY-MM-DD')
                                 AND t5.start_time <= v_end_date)
                         AND t1.create_time =
                             (SELECT MAX(tt1.create_time)
                                FROM viw_dams_wf_all_task tt1
                               WHERE tt1.procinst_id = t3.procinst_id)) ttt
               WHERE (ttt.sys_code = 'QXSQ' AND EXISTS
                      (SELECT 1
                         FROM dams_branch_relation br
                        WHERE ttt.stru_id = br.bnch_id
                          AND instr(v_qxsq_stru, br.up_bnch_id) > 0))
                  OR ttt.sys_code != 'QXSQ');
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT task_id,
                       task_name,
                       ssic_id,
                       '' user_name,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',  business_type, 'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       applyer,
                       apply_time,
                       complete_time,
                       close_flag,
                       row_number() over(ORDER BY create_time DESC) rn
                  FROM (SELECT t1.task_id,
                               t1.task_name,
                               t1.ssic_id,
                               '' user_name,
                               t1.stru_id,
                               t1.role_id,
                               t1.prev_task_id,
                               t1.prev_ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                               t1.prev_stru_id,
                               t1.prev_role_id,
                               t3.procinst_id,
                               t3.process_id,
                               t3.process_name,
                               t1.form_id,
                               t3.business_type,
                               t3.business_id,
                               t3.title,
                               t1.create_time,
                               t1.take_time,
                               t1.end_time,
                               t1.task_state,
                               t1.outgoing_name,
                               t1.opinion,
                               t3.creator_id applyer,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t3.creator_id) apply_name,
                               to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                               t3.complete_time,
                               t3.close_flag,
                               t3.sys_code
                          FROM viw_dams_wf_all_task    t1,
                               viw_dams_wf_all_process t3,
                               dams_user               t4
                         WHERE t1.procinst_id = t3.procinst_id
                              --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                           AND EXISTS
                         (SELECT '1'
                                  FROM dams_wf_process_conf t4
                                 WHERE t4.pdid = t3.process_id
                                   AND t4.sys_code =
                                       nvl(i_sys_code, t3.sys_code))
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_task t2
                                 WHERE t2.procinst_id = t3.procinst_id
                                   AND t2.ssic_id = i_ssic_id
                                   AND t2.task_state IN ('completed'))
                           AND t3.close_flag =
                               nvl(i_close_flag, t3.close_flag)
                              --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                           AND t3.creator_id = t4.ssic_id
                              --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                           AND t4.name = nvl(i_applyer, t4.name)
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_process t5
                                 WHERE t5.procinst_id = t3.procinst_id
                                   AND t5.start_time >=
                                       to_date(nvl(i_begin_date, v_min_date),
                                               'YYYY-MM-DD')
                                   AND t5.start_time <= v_end_date)
                           AND t1.create_time =
                               (SELECT MAX(tt1.create_time)
                                  FROM viw_dams_wf_all_task tt1
                                 WHERE tt1.procinst_id = t3.procinst_id)
                        UNION ALL
                        SELECT t1.task_id,
                               t1.task_name,
                               t1.ssic_id,
                               '' user_name,
                               t1.stru_id,
                               t1.role_id,
                               t1.prev_task_id,
                               t1.prev_ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                               t1.prev_stru_id,
                               t1.prev_role_id,
                               t3.procinst_id,
                               t3.process_id,
                               t3.process_name,
                               t1.form_id,
                               t3.business_type,
                               t3.business_id,
                               t3.title,
                               t1.create_time,
                               t1.take_time,
                               t1.end_time,
                               t1.task_state,
                               t1.outgoing_name,
                               t1.opinion,
                               t3.creator_id applyer,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t3.creator_id) apply_name,
                               to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                               t3.complete_time,
                               t3.close_flag,
                               t3.sys_code
                          FROM viw_dams_wf_all_task    t1,
                               viw_dams_wf_all_process t3,
                               dams_user               t4
                         WHERE t1.procinst_id = t3.procinst_id
                              --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                           AND EXISTS
                         (SELECT '1'
                                  FROM dams_wf_process_conf t4
                                 WHERE t4.pdid = t3.process_id
                                   AND t4.sys_code =
                                       nvl(i_sys_code, t3.sys_code))
                           AND NOT EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_task t2
                                 WHERE t2.procinst_id = t3.procinst_id
                                   AND t2.ssic_id = i_ssic_id
                                   AND t2.task_state IN ('completed'))
                           AND t3.close_flag =
                               nvl(i_close_flag, t3.close_flag)
                              --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                           AND t3.business_type IN
                               (SELECT DISTINCT t1.biz_type
                                  FROM dams_prm_biz_admin t1,
                                       dams_user_role_rel t2
                                 WHERE t1.biz_type IN
                                       (SELECT t3.dictcode
                                          FROM dams_gen_dict t3
                                         WHERE t3.dicttype = 'BIZ_TYPE'
                                           AND t3.sys_code = 'DAMS'
                                           AND t3.dictlang = 'zh_CN')
                                   AND t1.role = t2.role_id
                                   AND t2.ssic_id = i_ssic_id)

                           AND t3.creator_id = t4.ssic_id
                              --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                           AND t4.name = nvl(i_applyer, t4.name)
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_process t5
                                 WHERE t5.procinst_id = t3.procinst_id
                                   AND t5.start_time >=
                                       to_date(nvl(i_begin_date, v_min_date),
                                               'YYYY-MM-DD')
                                   AND t5.start_time <= v_end_date)
                           AND t1.create_time =
                               (SELECT MAX(tt1.create_time)
                                  FROM viw_dams_wf_all_task tt1
                                 WHERE tt1.procinst_id = t3.procinst_id)) ttt
                 WHERE (ttt.sys_code = 'QXSQ' AND EXISTS
                        (SELECT 1
                           FROM dams_branch_relation br
                          WHERE ttt.stru_id = br.bnch_id
                            AND instr(v_qxsq_stru, br.up_bnch_id) > 0))
                    OR ttt.sys_code != 'QXSQ'
                )
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = '3' THEN
      --sys_code不空 ,biz_type 空
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT 1
                FROM (SELECT t3.sys_code, t1.stru_id
                        FROM viw_dams_wf_all_task    t1,
                             viw_dams_wf_all_process t3,
                             dams_user               t4
                       WHERE t1.procinst_id = t3.procinst_id
                            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                         AND EXISTS (SELECT '1'
                                FROM dams_wf_process_conf t4
                               WHERE t4.pdid = t3.process_id
                                 AND t4.sys_code =
                                     nvl(i_sys_code, t3.sys_code))
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_task t2
                               WHERE t2.procinst_id = t3.procinst_id
                                 AND t2.ssic_id = i_ssic_id
                                 AND t2.task_state IN ('completed'))
                         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                         AND t3.business_type =
                             nvl(i_biz_type, t3.business_type)
                         AND t3.creator_id = t4.ssic_id
                            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                         AND t4.name = nvl(i_applyer, t4.name)
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_process t5
                               WHERE t5.procinst_id = t3.procinst_id
                                 AND t5.start_time >=
                                     to_date(nvl(i_begin_date, v_min_date),
                                             'YYYY-MM-DD')
                                 AND t5.start_time <= v_end_date)
                         AND t1.create_time =
                             (SELECT MAX(tt1.create_time)
                                FROM viw_dams_wf_all_task tt1
                               WHERE tt1.procinst_id = t3.procinst_id)
                      UNION ALL
                      SELECT t3.sys_code, t4.stru_id
                        FROM viw_dams_wf_all_task    t1,
                             viw_dams_wf_all_process t3,
                             dams_user               t4
                       WHERE t1.procinst_id = t3.procinst_id
                            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                         AND EXISTS (SELECT '1'
                                FROM dams_wf_process_conf t4
                               WHERE t4.pdid = t3.process_id
                                 AND t4.sys_code =
                                     nvl(i_sys_code, t3.sys_code))
                         AND NOT EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_task t2
                               WHERE t2.procinst_id = t3.procinst_id
                                 AND t2.ssic_id = i_ssic_id
                                 AND t2.task_state IN ('completed'))
                         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                            --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                         AND t3.business_type IN
                             (SELECT DISTINCT t1.biz_type
                                FROM dams_prm_biz_admin t1,
                                     dams_user_role_rel t2
                               WHERE t1.biz_type IN
                                     (SELECT t3.dictcode
                                        FROM dams_gen_dict t3
                                       WHERE t3.dicttype = 'BIZ_TYPE'
                                         AND t3.parent_code = i_sys_code
                                         AND t3.sys_code = 'DAMS'
                                         AND t3.dictlang = 'zh_CN')

                                 AND t1.role = t2.role_id
                                 AND t2.ssic_id = i_ssic_id)

                         AND t3.creator_id = t4.ssic_id
                            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                         AND t4.name = nvl(i_applyer, t4.name)
                         AND EXISTS
                       (SELECT '1'
                                FROM viw_dams_wf_all_process t5
                               WHERE t5.procinst_id = t3.procinst_id
                                 AND t5.start_time >=
                                     to_date(nvl(i_begin_date, v_min_date),
                                             'YYYY-MM-DD')
                                 AND t5.start_time <= v_end_date)
                         AND t1.create_time =
                             (SELECT MAX(tt1.create_time)
                                FROM viw_dams_wf_all_task tt1
                               WHERE tt1.procinst_id = t3.procinst_id)) ttt
               WHERE (ttt.sys_code = 'QXSQ' AND EXISTS
                      (SELECT 1
                         FROM dams_branch_relation br
                        WHERE ttt.stru_id = br.bnch_id
                          AND instr(v_qxsq_stru, br.up_bnch_id) > 0))
                  OR ttt.sys_code != 'QXSQ');
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT task_id,
                       task_name,
                       ssic_id,
                       user_name,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       applyer,
                       apply_time,
                       complete_time,
                       close_flag,
                       row_number() over(ORDER BY create_time DESC) rn
                  FROM (SELECT t1.task_id,
                               t1.task_name,
                               t1.ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.ssic_id) user_name,
                               t1.stru_id,
                               t1.role_id,
                               t1.prev_task_id,
                               t1.prev_ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                               t1.prev_stru_id,
                               t1.prev_role_id,
                               t3.procinst_id,
                               t3.process_id,
                               t3.process_name,
                               t1.form_id,
                               t3.business_type,
                               t3.business_id,
                               t3.title,
                               t1.create_time,
                               t1.take_time,
                               t1.end_time,
                               t1.task_state,
                               t1.outgoing_name,
                               t1.opinion,
                               t3.creator_id applyer,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t3.creator_id) apply_name,
                               to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                               t3.complete_time,
                               t3.close_flag,
                               t3.sys_code
                          FROM viw_dams_wf_all_task    t1,
                               viw_dams_wf_all_process t3,
                               dams_user               t4
                         WHERE t1.procinst_id = t3.procinst_id
                              --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                           AND EXISTS
                         (SELECT '1'
                                  FROM dams_wf_process_conf t4
                                 WHERE t4.pdid = t3.process_id
                                   AND t4.sys_code =
                                       nvl(i_sys_code, t3.sys_code))
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_task t2
                                 WHERE t2.procinst_id = t3.procinst_id
                                   AND t2.ssic_id = i_ssic_id
                                   AND t2.task_state IN ('completed'))
                           AND t3.close_flag =
                               nvl(i_close_flag, t3.close_flag)
                           AND t3.business_type =
                               nvl(i_biz_type, t3.business_type)
                           AND t3.creator_id = t4.ssic_id
                              --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                           AND t4.name = nvl(i_applyer, t4.name)
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_process t5
                                 WHERE t5.procinst_id = t3.procinst_id
                                   AND t5.start_time >=
                                       to_date(nvl(i_begin_date, v_min_date),
                                               'YYYY-MM-DD')
                                   AND t5.start_time <= v_end_date)
                           AND t1.create_time =
                               (SELECT MAX(tt1.create_time)
                                  FROM viw_dams_wf_all_task tt1
                                 WHERE tt1.procinst_id = t3.procinst_id)
                        UNION ALL
                        SELECT t1.task_id,
                               t1.task_name,
                               t1.ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.ssic_id) user_name,
                               t1.stru_id,
                               t1.role_id,
                               t1.prev_task_id,
                               t1.prev_ssic_id,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                               t1.prev_stru_id,
                               t1.prev_role_id,
                               t3.procinst_id,
                               t3.process_id,
                               t3.process_name,
                               t1.form_id,
                               t3.business_type,
                               t3.business_id,
                               t3.title,
                               t1.create_time,
                               t1.take_time,
                               t1.end_time,
                               t1.task_state,
                               t1.outgoing_name,
                               t1.opinion,
                               t3.creator_id applyer,
                               (SELECT u.name
                                  FROM dams_user u
                                 WHERE u.ssic_id = t3.creator_id) apply_name,
                               to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                               t3.complete_time,
                               t3.close_flag,
                               t3.sys_code
                          FROM viw_dams_wf_all_task    t1,
                               viw_dams_wf_all_process t3,
                               dams_user               t4
                         WHERE t1.procinst_id = t3.procinst_id
                              --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                           AND EXISTS
                         (SELECT '1'
                                  FROM dams_wf_process_conf t4
                                 WHERE t4.pdid = t3.process_id
                                   AND t4.sys_code =
                                       nvl(i_sys_code, t3.sys_code))
                           AND NOT EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_task t2
                                 WHERE t2.procinst_id = t3.procinst_id
                                   AND t2.ssic_id = i_ssic_id
                                   AND t2.task_state IN ('completed'))
                           AND t3.close_flag =
                               nvl(i_close_flag, t3.close_flag)
                              --AND T3.BUSINESS_TYPE = NVL(I_BIZ_TYPE, T3.BUSINESS_TYPE)
                           AND t3.business_type IN
                               (SELECT DISTINCT t1.biz_type
                                  FROM dams_prm_biz_admin t1,
                                       dams_user_role_rel t2
                                 WHERE t1.biz_type IN
                                       (SELECT t3.dictcode
                                          FROM dams_gen_dict t3
                                         WHERE t3.dicttype = 'BIZ_TYPE'
                                           AND t3.parent_code = i_sys_code
                                           AND t3.sys_code = 'DAMS'
                                           AND t3.dictlang = 'zh_CN')
                                   AND t1.role = t2.role_id
                                   AND t2.ssic_id = i_ssic_id)
                           AND t3.creator_id = t4.ssic_id
                              --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                           AND t4.name = nvl(i_applyer, t4.name)
                           AND EXISTS
                         (SELECT '1'
                                  FROM viw_dams_wf_all_process t5
                                 WHERE t5.procinst_id = t3.procinst_id
                                   AND t5.start_time >=
                                       to_date(nvl(i_begin_date, v_min_date),
                                               'YYYY-MM-DD')
                                   AND t5.start_time <= v_end_date)
                           AND t1.create_time =
                               (SELECT MAX(tt1.create_time)
                                  FROM viw_dams_wf_all_task tt1
                                 WHERE tt1.procinst_id = t3.procinst_id)) ttt
                 WHERE (ttt.sys_code = 'QXSQ' AND EXISTS
                        (SELECT 1
                           FROM dams_branch_relation br
                          WHERE ttt.stru_id = br.bnch_id
                            AND instr(v_qxsq_stru, br.up_bnch_id) > 0))
                    OR ttt.sys_code != 'QXSQ'
                )
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = '2' THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM viw_dams_wf_all_task    t1,
             viw_dams_wf_all_process t3,
             dams_user               t4
       WHERE t1.procinst_id = t3.procinst_id
            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
         AND EXISTS (SELECT '1'
                FROM dams_wf_process_conf t4
               WHERE t4.pdid = t3.process_id
                 AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
         AND t3.business_type = i_biz_type
         AND t3.creator_id = t4.ssic_id
            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
         AND t4.name = nvl(i_applyer, t4.name)
         AND EXISTS
       (SELECT '1'
                FROM viw_dams_wf_all_process t5
               WHERE t5.procinst_id = t3.procinst_id
                 AND t5.start_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t5.start_time <= v_end_date)
         AND t1.create_time =
             (SELECT MAX(tt1.create_time)
                FROM viw_dams_wf_all_task tt1
               WHERE tt1.procinst_id = t3.procinst_id)
         AND (t3.sys_code = 'QXSQ' AND EXISTS
              (SELECT 1
                 FROM dams_branch_relation br
                WHERE t1.stru_id = br.bnch_id
                  AND instr(v_qxsq_stru, br.up_bnch_id) > 0) OR
              t3.sys_code != 'QXSQ');
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT t1.task_id,
                       t1.task_name,
                       t1.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.ssic_id) user_name,
                       t1.stru_id,
                       t1.role_id,
                       t1.prev_task_id,
                       t1.prev_ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                       t1.prev_stru_id,
                       t1.prev_role_id,
                       t3.procinst_id,
                       t3.process_id,
                       t3.process_name,
                       t1.form_id,
                       t3.business_type,
                       t3.business_id,
                       t3.title,
                       t1.create_time,
                       t1.take_time,
                       t1.end_time,
                       t1.task_state,
                       t1.outgoing_name,
                       t1.opinion,
                       t3.creator_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t3.creator_id) apply_name,
                       to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                       t3.complete_time,
                       t3.close_flag,
                       t3.sys_code,
                       row_number() over(ORDER BY t1.create_time DESC) rn
                  FROM viw_dams_wf_all_task    t1,
                       viw_dams_wf_all_process t3,
                       dams_user               t4
                 WHERE t1.procinst_id = t3.procinst_id
                      --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t9
                         WHERE t9.pdid = t3.process_id
                           AND t9.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND t1.close_flag = nvl(i_close_flag, t1.close_flag)
                   AND t3.business_type = i_biz_type -- NVL(i_biz_type, T3.BUSINESS_TYPE)
                   AND t3.creator_id = t4.ssic_id
                      --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                   AND t4.name = nvl(i_applyer, t4.name)
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id)
                   AND (t3.sys_code = 'QXSQ' AND EXISTS
                        (SELECT 1
                           FROM dams_branch_relation br
                          WHERE t1.stru_id = br.bnch_id
                            AND instr(v_qxsq_stru, br.up_bnch_id) > 0) OR
                        t3.sys_code != 'QXSQ')
                 ORDER BY t1.create_time DESC) ttt
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = '1' THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM viw_dams_wf_all_task    t1,
             viw_dams_wf_all_process t3,
             dams_user               t4
       WHERE t1.procinst_id = t3.procinst_id
            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
         AND EXISTS (SELECT '1'
                FROM dams_wf_process_conf t4
               WHERE t4.pdid = t3.process_id
                 AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
         AND EXISTS
       (SELECT '1'
                FROM viw_dams_wf_all_task t2
               WHERE t2.procinst_id = t3.procinst_id
                 AND t2.ssic_id = i_ssic_id
                 AND t2.task_state IN ('completed'))
         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
         AND t3.business_type = nvl(i_biz_type, t3.business_type)
         AND t3.creator_id = t4.ssic_id
            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
         AND t4.name = nvl(i_applyer, t4.name)
         AND EXISTS
       (SELECT '1'
                FROM viw_dams_wf_all_process t5
               WHERE t5.procinst_id = t3.procinst_id
                 AND t5.start_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t5.start_time <= v_end_date)
         AND t1.create_time =
             (SELECT MAX(tt1.create_time)
                FROM viw_dams_wf_all_task tt1
               WHERE tt1.procinst_id = t3.procinst_id);
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT t1.task_id,
                       t1.task_name,
                       t1.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.ssic_id) user_name,
                       t1.stru_id,
                       t1.role_id,
                       t1.prev_task_id,
                       t1.prev_ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                       t1.prev_stru_id,
                       t1.prev_role_id,
                       t3.procinst_id,
                       t3.process_id,
                       t3.process_name,
                       t1.form_id,
                       t3.business_type,
                       t3.business_id,
                       t3.title,
                       t1.create_time,
                       t1.take_time,
                       t1.end_time,
                       t1.task_state,
                       t1.outgoing_name,
                       t1.opinion,
                       t3.creator_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t3.creator_id) apply_name,
                       to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                       t3.complete_time,
                       t3.close_flag,
                       row_number() over(ORDER BY t1.create_time DESC) rn
                  FROM viw_dams_wf_all_task    t1,
                       viw_dams_wf_all_process t3,
                       dams_user               t4
                 WHERE t1.procinst_id = t3.procinst_id
                      --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t9
                         WHERE t9.pdid = t3.process_id
                           AND t9.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_task t2
                         WHERE t2.procinst_id = t3.procinst_id
                           AND t2.ssic_id = i_ssic_id
                           AND t2.task_state IN ('completed'))
                   AND t1.close_flag = nvl(i_close_flag, t1.close_flag)
                   AND t3.business_type = nvl(i_biz_type, t3.business_type)
                   AND t3.creator_id = t4.ssic_id
                      --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                   AND t4.name = nvl(i_applyer, t4.name)
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id)
                 ORDER BY t1.create_time DESC)
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_task_qry;
  
  PROCEDURE proc_done_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_DONE_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
    v_inner_num NUMBER; --非流程待办数量
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,  pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
     WHERE t1.procinst_id = t4.procinst_id
       AND EXISTS
     (SELECT '1'
              FROM dams_wf_process_conf t3
             WHERE t3.pdid = t4.process_id
               AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
       AND (EXISTS (SELECT '1'
                      FROM dams_wf_task t2, dams_wf_participation p2
                     WHERE t2.procinst_id = p2.procinst_id
                       AND t2.prev_task_id = p2.pre_task_id
                       AND t2.procinst_id = t1.procinst_id
                       AND p2.ssic_id = i_ssic_id
                       AND t2.task_state IN ('completed')) OR EXISTS
            (SELECT '1'
               FROM dams_wf_task t2
              WHERE t2.procinst_id = t1.procinst_id
                AND t2.ssic_id = i_ssic_id
                AND t2.task_state IN ('completed')))
       AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
       AND t4.valid_flag = '1'
       AND t1.task_id = (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                           FROM dams_wf_task ttt1
                          WHERE ttt1.procinst_id = t4.procinst_id);
    --非流程待办
    SELECT COUNT(1)
      INTO v_inner_num
      FROM dams_inner_todo_task i
     WHERE i.ssic_id = i_ssic_id
       AND i.task_state = 'completed'
       AND i.sys_code = nvl(i_sys_code, i.sys_code)
       AND nvl(i.business_type, 0) = nvl(i_biz_type, nvl(i.business_type, 0));
    o_total_num := o_total_num + v_inner_num;
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             decode(task_name, '财务受理岗', '财务受理岗', user_name),
             role_id,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stru_name,
             url,
             in_sys_seq
        FROM (SELECT task_id,
                     pckg_dams_task.func_get_cur_task(business_id) task_name,
                     decode(role_id,
                            'INNER',
                            (SELECT listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
                               FROM dams_inner_todo_task t_open, dams_user u1
                              WHERE t_open.business_id = tmp.business_id
                                AND t_open.task_state = 'open'
                                AND t_open.ssic_id = u1.ssic_id(+)),
                            pckg_dams_task.func_get_cur_dealer(business_id)) user_name,
                     role_id,
                     stru_id,
                     prev_task_id,
                     prev_ssic_id,
                     prev_person_name,
                     prev_stru_id,
                     procinst_id,
                     process_id,
                     process_name,
                     form_id,
                     pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
                     business_id,
                     title,
                     create_time,
                     take_time,
                     end_time,
                     task_state,
                     outgoing_name,
                     opinion,
                     apply_name,
                     apply_time,
                     app_stru,
                     app_stru_name,
                     url,
                     in_sys_seq
                FROM (SELECT x.task_id,
                             x.task_name,
                             x.ssic_id,
                             x.user_name,
                             x.stru_id,
                             x.role_id,
                             x.prev_task_id,
                             x.prev_ssic_id,
                             x.prev_person_name,
                             x.prev_stru_id,
                             x.prev_role_id,
                             x.procinst_id,
                             x.process_id,
                             x.process_name,
                             x.form_id,
                             x.business_type,
                             x.business_id,
                             x.title,
                             x.create_time,
                             x.take_time,
                             x.end_time,
                             x.task_state,
                             x.outgoing_name,
                             x.opinion,
                             x.applyer,
                             x.apply_name,
                             x.apply_time,
                             x.app_stru,
                             x.app_stru_name,
                             x.url,
                             x.in_sys_seq,
                             row_number() over(ORDER BY x.create_time DESC) rn
                        FROM (SELECT to_char(t1.task_id) task_id,
                                     t1.task_name,
                                     NULL ssic_id,
                                     NULL user_name,
                                     t1.stru_id,
                                     t1.role_id,
                                     t1.prev_task_id,
                                     t1.prev_ssic_id,
                                     (SELECT u.name
                                        FROM dams_user u
                                       WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                     t1.prev_stru_id,
                                     t1.prev_role_id,
                                     t4.procinst_id,
                                     t4.process_id,
                                     t4.process_name,
                                     t1.form_id,
                                     t4.business_type,
                                     t4.business_id,
                                     t4.title,
                                     t1.create_time,
                                     t1.take_time,
                                     t1.end_time,
                                     t1.task_state,
                                     t1.outgoing_name,
                                     t1.opinion,
                                     t4.creator_id applyer,
                                     (SELECT u.name
                                        FROM dams_user u
                                       WHERE u.ssic_id = t4.creator_id) apply_name,
                                     to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                     t4.creator_stru app_stru,
                                     (SELECT b.stru_sname
                                        FROM dams_branch b
                                       WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                     NULL url,
                                     NULL in_sys_seq
                                FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                               WHERE t1.procinst_id = t4.procinst_id
                                 AND EXISTS
                               (SELECT '1'
                                        FROM dams_wf_process_conf t3
                                       WHERE t3.pdid = t4.process_id
                                         AND t3.sys_code =
                                             nvl(i_sys_code, t3.sys_code))
                                 AND (EXISTS
                                      (SELECT '1'
                                         FROM dams_wf_task          t2,
                                              dams_wf_participation p2
                                        WHERE t2.procinst_id = p2.procinst_id
                                          AND t2.prev_task_id = p2.pre_task_id
                                          AND t2.procinst_id = t1.procinst_id
                                          AND p2.ssic_id = i_ssic_id
                                          AND t2.task_state IN ('completed')) OR
                                      EXISTS
                                      (SELECT '1'
                                         FROM dams_wf_task t2
                                        WHERE t2.procinst_id = t1.procinst_id
                                          AND t2.ssic_id = i_ssic_id
                                          AND t2.task_state IN ('completed')))
                                 AND nvl(t4.business_type, 0) =
                                     nvl(i_biz_type, nvl(t4.business_type, 0))
                                 AND t4.valid_flag = '1'
                                 AND t1.task_id =
                                     (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                                        FROM dams_wf_task ttt1
                                       WHERE ttt1.procinst_id = t4.procinst_id)
                              UNION ALL
                              SELECT i.task_id,
                                     NULL task_name,
                                     i.ssic_id,
                                     (SELECT u.name
                                        FROM dams_user u
                                       WHERE u.ssic_id = i.ssic_id) user_name,
                                     NULL stru_id,
                                     'INNER' role_id, --非流程待办
                                     NULL prev_task_id,
                                     i.prev_ssic_id,
                                     (SELECT u.name
                                        FROM dams_user u
                                       WHERE u.ssic_id = i.prev_ssic_id) prev_person_name,
                                     NULL prev_stru_id,
                                     NULL prev_role_id,
                                     NULL procinst_id,
                                     NULL process_id,
                                     NULL process_name,
                                     NULL form_id,
                                     i.business_type,
                                     i.business_id,
                                     i.title,
                                     i.create_time,
                                     i.take_time,
                                     i.end_time,
                                     i.task_state,
                                     NULL outgoing_name,
                                     NULL opinion,
                                     i.creator_id applyer,
                                     (SELECT u.name
                                        FROM dams_user u
                                       WHERE u.ssic_id = i.creator_id) apply_name,
                                     to_char(i.create_time, 'YYYY-MM-DD') apply_time,
                                     i.creator_stru app_stru,
                                     (SELECT b.stru_sname
                                        FROM dams_branch b
                                       WHERE b.stru_id = i.creator_stru) app_stru_name,
                                     i.url,
                                     i.in_sys_seq
                                FROM dams_inner_todo_task i
                               WHERE i.ssic_id = i_ssic_id
                                 AND i.task_state = 'completed'
                                 AND i.sys_code = nvl(i_sys_code, i.sys_code)
                                 AND nvl(i.business_type, 0) =
                                     nvl(i_biz_type, nvl(i.business_type, 0))) x) tmp
               WHERE rn >= to_number(i_beg_num)
                 AND rn < to_number(i_beg_num) + to_number(i_fetch_num));
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_done_task_qry;
  
  PROCEDURE proc_over_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_biz_type  IN VARCHAR2, --业务类型
                               i_sys_code  IN VARCHAR2, --系统编码
                               i_beg_num   IN VARCHAR2, --查询开始记录
                               i_fetch_num IN VARCHAR2, --查询记录数
                               o_retcode   OUT VARCHAR2, --返回值
                               o_total_num OUT VARCHAR2, --记录总数
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_OVER_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
    v_inner_num NUMBER; --非流程待办数量
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_hist_task t1, dams_wf_hist_process t4 --    申请任务
     WHERE t1.procinst_id = t4.procinst_id
       AND EXISTS
     (SELECT '1'
              FROM dams_wf_process_conf t3
             WHERE t3.pdid = t4.process_id
               AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
       AND (EXISTS (SELECT '1'
                      FROM dams_wf_hist_task t2, dams_wf_participation p2
                     WHERE t2.procinst_id = p2.procinst_id
                       AND t2.prev_task_id = p2.pre_task_id
                       AND t2.procinst_id = t1.procinst_id
                       AND p2.ssic_id = i_ssic_id) OR EXISTS
            (SELECT 1
               FROM dams_wf_hist_task t2
              WHERE t2.ssic_id = i_ssic_id
                AND t2.procinst_id = t4.procinst_id))
       AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
       AND (t1.create_time =
           (SELECT MAX(ttt1.create_time)
               FROM dams_wf_hist_task ttt1
              WHERE ttt1.procinst_id = t4.procinst_id) OR
           (t1.is_first = '1' AND
           (SELECT MAX(ttt1.create_time)
                FROM dams_wf_hist_task ttt1
               WHERE ttt1.procinst_id = t4.procinst_id) IS NULL))
       AND t4.valid_flag = '1';
    --非流程待办
    SELECT COUNT(1)
      INTO v_inner_num
      FROM dams_inner_over_task i
     WHERE i.ssic_id = i_ssic_id
       AND i.task_state = 'completed'
       AND i.sys_code = nvl(i_sys_code, i.sys_code)
       AND nvl(i.business_type, 0) = nvl(i_biz_type, nvl(i.business_type, 0));
    o_total_num := o_total_num + v_inner_num;
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             role_id,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             complete_time,
             app_stru,
             app_stru_name,
             url,
             in_sys_seq
        FROM (SELECT x.task_id,
                     x.task_name,
                     x.ssic_id,
                     x.user_name,
                     x.stru_id,
                     x.role_id,
                     x.prev_task_id,
                     x.prev_ssic_id,
                     x.prev_person_name,
                     x.prev_stru_id,
                     x.prev_role_id,
                     x.procinst_id,
                     x.process_id,
                     x.process_name,
                     x.form_id,
                     x.business_type,
                     x.business_id,
                     x.title,
                     x.create_time,
                     x.take_time,
                     x.end_time,
                     x.task_state,
                     x.outgoing_name,
                     x.opinion,
                     x.applyer,
                     x.apply_name,
                     x.apply_time,
                     x.complete_time,
                     x.app_stru,
                     x.app_stru_name,
                     x.url,
                     x.in_sys_seq,
                     row_number() over(ORDER BY nvl(x.create_time, x.end_time) DESC) rn
                FROM (SELECT to_char(t1.task_id) task_id,
                              t1.task_name,
                              t1.ssic_id,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = t1.ssic_id) user_name,
                              t1.stru_id,
                              t1.role_id,
                              t1.prev_task_id,
                              t1.prev_ssic_id,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                              t1.prev_stru_id,
                              t1.prev_role_id,
                              t4.procinst_id,
                              t4.process_id,
                              t4.process_name,
                              t1.form_id,
                              t4.business_type,
                              t4.business_id,
                              t4.title,
                              t1.create_time,
                              t1.take_time,
                              t1.end_time,
                              t1.task_state,
                              t1.outgoing_name,
                              t1.opinion,
                              t4.creator_id applyer,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = t4.creator_id) apply_name,
                              to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                              to_char(t4.complete_time, 'YYYY-MM-DD') complete_time,
                              t4.creator_stru app_stru,
                              (SELECT b.stru_sname
                                 FROM dams_branch b
                                WHERE b.stru_id = t4.creator_stru) app_stru_name,
                              '' url,
                              '' in_sys_seq
                         FROM dams_wf_hist_task t1, dams_wf_hist_process t4 --    申请任务
                        WHERE t1.procinst_id = t4.procinst_id
                     AND EXISTS
                        (SELECT '1'
                           FROM dams_wf_process_conf t3
                          WHERE t3.pdid = t4.process_id
                            AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND (EXISTS
                           (SELECT '1'
                             FROM dams_wf_hist_task   t2,
                                  dams_wf_participation p2
                            WHERE t2.procinst_id = p2.procinst_id
                              AND t2.prev_task_id = p2.pre_task_id
                              AND t2.procinst_id = t1.procinst_id
                              AND p2.ssic_id = i_ssic_id) OR
                            EXISTS (SELECT 1
                           FROM dams_wf_hist_task t2
                          WHERE t2.ssic_id = i_ssic_id
                            AND t2.procinst_id = t4.procinst_id))
                     AND nvl(t4.business_type, 0) =
                        nvl(i_biz_type, nvl(t4.business_type, 0))
                     AND (t1.create_time =
                        (SELECT MAX(ttt1.create_time)
                            FROM dams_wf_hist_task ttt1
                           WHERE ttt1.procinst_id = t4.procinst_id) OR
                        (t1.is_first = '1' AND
                        (SELECT MAX(ttt1.create_time)
                             FROM dams_wf_hist_task ttt1
                            WHERE ttt1.procinst_id = t4.procinst_id) IS NULL))
                     AND t4.valid_flag = '1'
                       UNION ALL
                       SELECT i.task_id,
                              NULL task_name,
                              i.ssic_id,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = i.ssic_id) user_name,
                              NULL stru_id,
                              'INNER' role_id, --非流程待办
                              NULL prev_task_id,
                              i.prev_ssic_id,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = i.prev_ssic_id) prev_person_name,
                              NULL prev_stru_id,
                              NULL prev_role_id,
                              NULL procinst_id,
                              NULL process_id,
                              NULL process_name,
                              NULL form_id,
                              i.business_type,
                              i.business_id,
                              i.title,
                              i.create_time,
                              i.take_time,
                              i.end_time,
                              i.task_state,
                              NULL outgoing_name,
                              NULL opinion,
                              i.creator_id applyer,
                              (SELECT u.name
                                 FROM dams_user u
                                WHERE u.ssic_id = i.creator_id) apply_name,
                              NULL apply_time,
                              to_char(i.end_time, 'YYYY-MM-DD') complete_time,
                              i.creator_stru app_stru,
                              (SELECT b.stru_sname
                                 FROM dams_branch b
                                WHERE b.stru_id = i.creator_stru) app_stru_name,
                              i.url,
                              i.in_sys_seq
                         FROM dams_inner_over_task i
                        WHERE i.ssic_id = i_ssic_id
                          AND i.task_state = 'completed'
                          AND i.sys_code = nvl(i_sys_code, i.sys_code)
                          AND nvl(i.business_type, 0) =
                              nvl(i_biz_type, nvl(i.business_type, 0))
                       ) x)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_over_task_qry;
  
  PROCEDURE proc_trans_task_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                i_biz_type  IN VARCHAR2, --业务类型
                                i_sys_code  IN VARCHAR2, --系统编码
                                i_beg_num   IN VARCHAR2, --查询开始记录
                                i_fetch_num IN VARCHAR2, --查询记录数
                                o_retcode   OUT VARCHAR2, --返回值
                                o_total_num OUT VARCHAR2, --记录总数
                                o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TRANS_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_process t4
     WHERE --T4.SYS_CODE = NVL(I_SYS_CODE, T4.SYS_CODE)
     EXISTS (SELECT '1'
        FROM dams_wf_process_conf t3
       WHERE t3.pdid = t4.process_id
         AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
     AND EXISTS
     (SELECT '1'
        FROM dams_wf_task t2
       WHERE t2.procinst_id = t4.procinst_id
         AND t2.ssic_id_tran = i_ssic_id
         AND t2.auth_state = 1
         AND t2.task_state IN ('completed'))
     AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
     AND t4.valid_flag = '1';
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stru_name
        FROM (SELECT t1.task_id,
                      t1.task_name,
                      nvl(t1.ssic_id,
                          (SELECT wm_concat(t4.userid_)
                             FROM jbpm4_participation t4
                            WHERE t4.task_ = t1.task_id)) ssic_id,
                      nvl(pckg_dams_util.func_getusernamebyid(t1.ssic_id),
                          (SELECT wm_concat(pckg_dams_util.func_getusernamebyid(t4.userid_))
                             FROM jbpm4_participation t4
                            WHERE t4.task_ = t1.task_id)) user_name,
                      t1.stru_id,
                      t1.role_id,
                      t1.prev_task_id,
                      t1.prev_ssic_id,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                      t1.prev_stru_id,
                      t1.prev_role_id,
                      t4.procinst_id,
                      t4.process_id,
                      t4.process_name,
                      t1.form_id,
                      pckg_dams_util.func_decode('BIZ_TYPE', t4.business_type, 'DAMS') business_type,
                      t4.business_id,
                      t4.title,
                      t1.create_time,
                      t1.take_time,
                      t1.end_time,
                      t1.task_state,
                      t1.outgoing_name,
                      t1.opinion,
                      t4.creator_id applyer,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t4.creator_id) apply_name,
                      to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                      t4.creator_stru app_stru,
                      (SELECT b.stru_sname
                         FROM dams_branch b
                        WHERE b.stru_id = t4.creator_stru) app_stru_name,
                      row_number() over(ORDER BY t1.create_time DESC) rn
                 FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                WHERE --T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                EXISTS (SELECT '1'
                   FROM dams_wf_process_conf t3
                  WHERE t3.pdid = t4.process_id
                    AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
             AND t1.procinst_id = t4.procinst_id
             AND EXISTS (SELECT '1'
                   FROM dams_wf_task t2
                  WHERE t2.procinst_id = t1.procinst_id
                    AND t2.ssic_id_tran = i_ssic_id
                    AND t2.auth_state = '1'
                    AND t2.task_state IN ('completed'))
               --AND T1.IS_FIRST = 0
               --AND T1.TASK_STATE IN ('open', 'suspended')
             AND nvl(t4.business_type, 0) =
                nvl(i_biz_type, nvl(t4.business_type, 0))
             AND t1.create_time =
                (SELECT MAX(ttt1.create_time)
                   FROM dams_wf_task ttt1
                  WHERE ttt1.procinst_id = t4.procinst_id)
             AND t4.valid_flag = '1'
                ORDER BY t1.create_time DESC)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_trans_task_qry;
  
  PROCEDURE proc_form_url_qry(i_form_id  IN VARCHAR2, --FORM_ID
                              i_type     IN VARCHAR2, --类型 1,审批，2，只读，3，申请
                              o_retcode  OUT VARCHAR2, --返回值
                              o_form_url OUT VARCHAR2 --form的URL
                              ) IS
    v_param     db_log.info%TYPE := i_form_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_FORM_URL_QRY';
    v_step      db_log.step_no%TYPE := 0;
    v_form_sql  VARCHAR2(4000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_type = '1' THEN
      v_form_sql := 'form_url';
    ELSIF i_type = '2' THEN
      v_form_sql := 'read_url';
    ELSIF i_type = '3' THEN
      v_form_sql := 'app_url';
    ELSIF i_type ='4' THEN
      v_form_sql := 'RES_URL';
    END IF;
    EXECUTE IMMEDIATE '
    SELECT t2.' || v_form_sql || '
    FROM dams_form t2
    WHERE t2.form_id = ''' || i_form_id || ''''
      INTO o_form_url;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_form_url_qry;
  
  PROCEDURE proc_task_dealed_qry(i_ssic_id IN VARCHAR2, --用户id
                                 i_task_id IN VARCHAR2, --taskid
                                 o_retcode OUT VARCHAR2 --返回值 0,未处理过，1，处理过
                                 ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_task_id;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TASK_DEALED_QRY';
    v_step        db_log.step_no%TYPE := 0;
    v_business_id VARCHAR2(4000);
  BEGIN
    pack_log.info(v_proc_name,pack_log.start_step,pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT t.business_id
      INTO v_business_id
      FROM dams_wf_task t
     WHERE t.task_id = i_task_id;
    SELECT decode(COUNT(1), 0, 0, 1)
      INTO o_retcode
      FROM dams_wf_task t
     WHERE t.business_id = v_business_id
       AND t.ssic_id = i_ssic_id
       AND t.task_id != i_task_id;
    pack_log.info(v_proc_name,pack_log.end_step,pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_task_dealed_qry;
  
  PROCEDURE proc_del_task(i_ssic_id IN VARCHAR2, --统一认证号
                          i_app_id  IN VARCHAR2, --业务类型
                          i_valid   IN VARCHAR2, --类型，0，废止，1 生效
                          o_retcode OUT VARCHAR2, --返回值
                          o_msg     OUT VARCHAR2 --返回信息
                          ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_app_id;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_DEL_TASK';
    v_step        db_log.step_no%TYPE := 0;
    v_count       NUMBER;
    v_min_task_id NUMBER;
    v_max_task_id NUMBER;
    e_custom EXCEPTION;
    v_valid     VARCHAR2(2); --有效,1有效
    v_app_valid VARCHAR2(2); --申请有效，0有效
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_valid = '0' THEN
      --废止
      v_valid     := '0';
      v_app_valid := '1';
    ELSE
      --恢复
      v_valid     := '1';
      v_app_valid := '0';
    END IF;
    --删除草稿
    UPDATE dams_save_info t
       SET t.valid_flag = v_valid
     WHERE t.app_id = i_app_id;

    --若非草稿判断是否可以废止申请
    SELECT COUNT(1)
      INTO v_count
      FROM dams_wf_process t
     WHERE t.business_id = i_app_id;
    IF v_count > 0 THEN
      --存在申请，继续操作
      SELECT MIN(t.task_id)
        INTO v_min_task_id
        FROM dams_wf_task t
       WHERE t.business_id = i_app_id
         AND t.is_first = 1;

      SELECT MAX(t.task_id)
        INTO v_max_task_id
        FROM dams_wf_task t
       WHERE t.business_id = i_app_id
         AND t.task_state IN ('open', 'drop');

      SELECT decode(t1.task_name, t2.task_name, 1, 0)
        INTO v_count
        FROM dams_wf_task t1, dams_wf_task t2
       WHERE t1.task_id = v_min_task_id
         AND t2.task_id = v_max_task_id;
      --法审送审 FLSC 和 SUP 无需判断是否申请人环节 20150121 modified
      IF v_count = 1 OR instr(i_app_id, 'FLSC') = 1 OR instr(i_app_id, 'SUP') = 1 THEN
        --申请任务名与当前任务名一致，视为被否决到最初节点
        UPDATE dams_wf_process t
           SET t.valid_flag = v_valid
         WHERE t.business_id = i_app_id;
        IF i_valid = '0' THEN
          --add by linlc --解决生产bug:废止申请没有同步网讯待办
          INSERT INTO dams_webservice_del_task
            (keyid, create_time, send_flag)
            SELECT t.task_id || '_' || t3.userid_, SYSDATE, '0'
              FROM dams_wf_task t, jbpm4_participation t3
             WHERE t.task_id = v_max_task_id
               AND t.task_state IN ('open', 'suspended')
               AND t.task_id = t3.task_;
          --废止申请
          UPDATE dams_wf_task t
             SET t.task_state = 'drop'
           WHERE t.task_id = v_max_task_id
             AND t.task_state IN ('open', 'suspended');
        ELSE
          UPDATE dams_wf_task t
             SET t.task_state = 'open', t.issend = '0'
           WHERE t.task_id = v_max_task_id
             AND t.task_state IN ('drop');
        END IF;
        --更新各业务表
        UPDATE sa_dev_apply t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_hc_apply t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_hct_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_dev_wh_apply t --设备维修
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_dev_scrap_apply t --设备报废
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_sys_wh_apply t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_sec_excep_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_sec_exam_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_mail_excep_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sa_pub_soft_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;

        UPDATE sd_office_sup_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;

        UPDATE se_print_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;

        UPDATE sf_card_chg_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sf_card_new_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
        UPDATE sf_card_renew_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;

        UPDATE sg_uc_cancel_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;
		 
        UPDATE sg_uc_new_app t
           SET t.valid_flag = v_app_valid
         WHERE t.app_id = i_app_id;

        --20141119 ADDED
        UPDATE sy_petition_letter_info t
           SET t.valid_flag = v_valid
         WHERE t.app_id = i_app_id;

        UPDATE sy_petition_letter_drf t
           SET t.valid_flag = v_valid
         WHERE t.app_id = i_app_id;

        UPDATE sy_supervise_case_info t
           SET t.valid_flag = v_valid
         WHERE t.app_id = i_app_id;
      ELSE
        o_retcode := '1';
        o_msg     := '该任务不能被废止/恢复';
        RAISE e_custom;
      END IF;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN e_custom THEN
      pack_log.info(v_proc_name, pack_log.end_step, o_msg || v_param);
      RETURN;
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_del_task;
  
  PROCEDURE proc_is_process_dealed(i_ssic_id    IN VARCHAR2, --统一认证号
                                   i_app_id     IN VARCHAR2, --业务类型
                                   o_retcode    OUT VARCHAR2, --返回值
                                   o_msg        OUT VARCHAR2, --返回信息
                                   o_is_process OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_app_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_IS_PROCESS_DEALED';
    v_step      db_log.step_no%TYPE := 0;
    e_custom EXCEPTION;
    v_count NUMBER;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    v_count   := pckg_dams_task.func_is_biz_admin(i_app_id, i_ssic_id);
    IF v_count != 1 THEN
      o_is_process := pckg_dams_task.func_is_process_dealed(i_app_id, i_ssic_id);
    ELSE
      o_is_process := v_count;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN e_custom THEN
      pack_log.info(v_proc_name, pack_log.end_step, o_msg || v_param);
      RETURN;
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_is_process_dealed;
  
  PROCEDURE proc_is_process_closed(i_app_id    IN VARCHAR2, --申请编号
                                   o_ret_code  OUT VARCHAR2, --返回值
                                   o_msg       OUT VARCHAR2,
                                   o_closeflag OUT VARCHAR2) IS
    v_param db_log.info%TYPE := i_app_id;

    v_count NUMBER(5);

    v_proc_name db_log.proc_name%TYPE := 'pckg_ss_common.proc_is_process_closed';
  BEGIN
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);

    o_ret_code  := '0';
    o_closeflag := 'N';
    SELECT COUNT(1)
      INTO v_count
      FROM dams_wf_hist_process t
     WHERE t.business_id = i_app_id;
    IF v_count > 0 THEN
      o_closeflag := 'Y';
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code  := '-1';
      o_closeflag := 'N';
      o_msg       := SQLERRM;
      ROLLBACK;
  END proc_is_process_closed;
  
  PROCEDURE proc_is_todo_task_exist(i_ssic_id     IN VARCHAR2, --登陆人
                                    i_task_id     IN VARCHAR2, --任务ID
                                    i_business_id IN VARCHAR2, --业务主键
                                    o_retcode     OUT VARCHAR2, --返回值
                                    o_msg         OUT VARCHAR2,
                                    o_isexist     OUT VARCHAR2 --待办是否还在
                                    ) IS
    v_param     db_log.info%TYPE := i_task_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_IS_TODO_TASK_EXIST';
    v_step      db_log.step_no%TYPE := 0;
    v_count NUMBER;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO v_count
      FROM viw_dams_wf_all_process t, viw_dams_wf_all_task a
     WHERE t.business_id = i_business_id
       AND t.business_id = a.business_id;
    IF v_count > 0 THEN
      o_isexist := 'Y';
      o_msg     := '该记录存在';
    ELSE
      o_isexist := 'N';
      o_msg     := '该待办记录已被处理或被上一处理人取回';
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_is_todo_task_exist;
  
  PROCEDURE proc_todo_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TODO_TASK_SJ_QRY';
    v_step      db_log.step_no%TYPE := 0;
    v_sav_num   NUMBER;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_task t1, dams_wf_process t4, sj_gov_file_app a
     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
       AND t4.business_id = a.app_id
       AND EXISTS (SELECT '1'
              FROM jbpm4_participation t2
             WHERE t2.userid_ = i_ssic_id
               AND t2.task_ = t1.task_id)
       AND t1.procinst_id = t4.procinst_id
          --AND T1.IS_FIRST = 1
       AND t1.task_state IN ('open', 'suspended')
       AND nvl(t4.business_type, 0) =
           nvl(i_biz_type, nvl(t4.business_type, 0))
       AND t4.valid_flag = '1';
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             prev_task_id,
             prev_task_name,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stru_name,
             cfg_id,
             docno,
             title1,
             from_stru,
             from_stru_name,
             spec,
             spec_name
        FROM (SELECT t1.task_id,
                     t1.task_name,
                     t1.ssic_id,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t1.ssic_id) user_name,
                     t1.stru_id,
                     t1.role_id,
                     t1.prev_task_id,
                     (SELECT wt.task_name
                        FROM dams_wf_task wt
                       WHERE wt.task_id = t1.prev_task_id) prev_task_name,
                     t1.prev_ssic_id,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                     t1.prev_stru_id,
                     t1.prev_role_id,
                     t4.procinst_id,
                     t4.process_id,
                     t4.process_name,
                     t1.form_id,
                     t4.business_type,
                     t4.business_id,
                     t4.title,
                     to_char(t1.create_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
                     to_char(t1.take_time, 'YYYY-MM-DD') take_time,
                     to_char(t1.end_time, 'YYYY-MM-DD') end_time,
                     t1.task_state,
                     t1.outgoing_name,
                     t1.opinion,
                     t4.creator_id applyer,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t4.creator_id) apply_name,
                     to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                     t4.creator_stru app_stru,
                     (SELECT b.stru_sname
                        FROM dams_branch b
                       WHERE b.stru_id = t4.creator_stru) app_stru_name,
                     t4.cfg_id cfg_id,
                     a.docno,
                     a.title title1,
                     a.from_stru,
                     (SELECT t5.dictvalue
                        FROM dams_gen_dict t5
                       WHERE t5.dicttype = 'SJFILE_FROM_STRU'
                         AND t5.sys_code = 'SJ'
                         AND t5.dictcode = a.from_stru
                         AND t5.dictlang = 'zh_CN') from_stru_name,
                     a.spec,
                     (SELECT t6.dictvalue
                        FROM dams_gen_dict t6
                       WHERE t6.dicttype = 'SJFILE_SPEC'
                         AND t6.sys_code = 'SJ'
                         AND t6.dictcode = a.spec
                         AND t6.dictlang = 'zh_CN') spec_name,
                     --A.APP_STRU,
                     row_number() over(ORDER BY t1.create_time DESC) rn
                FROM dams_wf_task    t1,
                     dams_wf_process t4, --    申请任务
                     sj_gov_file_app a
               WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
                 AND t1.business_id = a.app_id
                 AND EXISTS (SELECT '1'
                        FROM jbpm4_participation t2
                       WHERE t2.userid_ = i_ssic_id
                         AND t2.task_ = t1.task_id)
                 AND t1.procinst_id = t4.procinst_id
                    --AND T1.IS_FIRST = 1
                 AND t1.task_state IN ('open', 'suspended')
                 AND nvl(t4.business_type, 0) =
                     nvl(i_biz_type, nvl(t4.business_type, 0))
                 AND t4.valid_flag = '1')
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_todo_task_sj_qry;
  
  PROCEDURE proc_done_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_DONE_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_process t4, sj_gov_file_app a
     WHERE --T4.SYS_CODE = NVL(I_SYS_CODE, T4.SYS_CODE)
     t4.business_id = a.app_id
     AND EXISTS (SELECT '1'
        FROM dams_wf_process_conf t3
       WHERE t3.pdid = t4.process_id
         AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
     AND t4.complete_time IS NULL
     AND EXISTS
     (SELECT '1'
        FROM dams_wf_task t2
       WHERE t2.procinst_id = t4.procinst_id
         AND t2.ssic_id = i_ssic_id
         AND t2.task_state IN ('completed'))
    --AND T1.IS_FIRST = 0
    --AND T1.TASK_STATE IN ('open', 'suspended')
     AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
     AND t4.valid_flag = '1';
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             pckg_dams_task.func_get_cur_dealer(business_id) user_name,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stru_name,
             docno,
             title1,
             from_stru,
             from_stru_name,
             spec,
             spec_name
        FROM (SELECT t1.task_id,
                     t1.task_name,
                     '' ssic_id,
                     '' user_name,
                     t1.stru_id,
                     t1.role_id,
                     t1.prev_task_id,
                     t1.prev_ssic_id,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                     t1.prev_stru_id,
                     t1.prev_role_id,
                     t4.procinst_id,
                     t4.process_id,
                     t4.process_name,
                     t1.form_id,
                     t4.business_type,
                     t4.business_id,
                     t4.title,
                     t1.create_time,
                     t1.take_time,
                     t1.end_time,
                     t1.task_state,
                     t1.outgoing_name,
                     t1.opinion,
                     t4.creator_id applyer,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t4.creator_id) apply_name,
                     to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                     t4.creator_stru app_stru,
                     (SELECT b.stru_sname
                        FROM dams_branch b
                       WHERE b.stru_id = t4.creator_stru) app_stru_name,
                     a.docno,
                     a.title title1,
                     a.from_stru,
                     (SELECT t5.dictvalue
                        FROM dams_gen_dict t5
                       WHERE t5.dicttype = 'SJFILE_FROM_STRU'
                         AND t5.sys_code = 'SJ'
                         AND t5.dictcode = a.from_stru
                         AND t5.dictlang = 'zh_CN') from_stru_name,
                     a.spec,
                     (SELECT t6.dictvalue
                        FROM dams_gen_dict t6
                       WHERE t6.dicttype = 'SJFILE_SPEC'
                         AND t6.sys_code = 'SJ'
                         AND t6.dictcode = a.spec
                         AND t6.dictlang = 'zh_CN') spec_name,
                     row_number() over(ORDER BY t1.create_time DESC) rn
                FROM dams_wf_task    t1,
                     dams_wf_process t4, --    申请任务
                     sj_gov_file_app a
               WHERE t4.business_id = a.app_id
                 AND EXISTS
               (SELECT '1'
                        FROM dams_wf_process_conf t3
                       WHERE t3.pdid = t4.process_id
                         AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
                    --T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                 AND t1.procinst_id = t4.procinst_id
                 AND EXISTS (SELECT '1'
                        FROM dams_wf_task t2
                       WHERE t2.procinst_id = t1.procinst_id
                         AND t2.ssic_id = i_ssic_id
                         AND t2.task_state IN ('completed'))
                 AND nvl(t4.business_type, 0) =
                     nvl(i_biz_type, nvl(t4.business_type, 0))
                 AND t4.valid_flag = '1'
                 AND t1.create_time =
                     (SELECT MAX(ttt1.create_time)
                        FROM dams_wf_task ttt1
                       WHERE ttt1.procinst_id = t4.procinst_id
                         AND ttt1.task_state = 'open')
               ORDER BY t1.create_time DESC)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_done_task_sj_qry;
  
  PROCEDURE proc_over_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_OVER_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_hist_process t4, sj_gov_file_app a
     WHERE --T4.SYS_CODE = NVL(I_SYS_CODE, T4.SYS_CODE)
     t4.business_id = a.app_id
     AND EXISTS (SELECT '1'
        FROM dams_wf_process_conf t3
       WHERE t3.pdid = t4.process_id
         AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
     AND EXISTS
     (SELECT 1
        FROM dams_wf_hist_task t2
       WHERE t2.ssic_id = i_ssic_id
         AND t2.procinst_id = t4.procinst_id)
     AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
     AND t4.valid_flag = '1';
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             complete_time,
             app_stru,
             app_stru_name,
             docno,
             title1,
             from_stru,
             from_stru_name,
             spec,
             spec_name
        FROM (SELECT t1.task_id,
                      t1.task_name,
                      t1.ssic_id,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t1.ssic_id) user_name,
                      t1.stru_id,
                      t1.role_id,
                      t1.prev_task_id,
                      t1.prev_ssic_id,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                      t1.prev_stru_id,
                      t1.prev_role_id,
                      t4.procinst_id,
                      t4.process_id,
                      t4.process_name,
                      t1.form_id,
                      t4.business_type,
                      t4.business_id,
                      t4.title,
                      t1.create_time,
                      t1.take_time,
                      t1.end_time,
                      t1.task_state,
                      t1.outgoing_name,
                      t1.opinion,
                      t4.creator_id applyer,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t4.creator_id) apply_name,
                      to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                      to_char(t4.complete_time, 'YYYY-MM-DD') complete_time,
                      t4.creator_stru app_stru,
                      (SELECT b.stru_sname
                         FROM dams_branch b
                        WHERE b.stru_id = t4.creator_stru) app_stru_name,
                      a.docno,
                      a.title title1,
                      a.from_stru,
                      (SELECT t5.dictvalue
                         FROM dams_gen_dict t5
                        WHERE t5.dicttype = 'SJFILE_FROM_STRU'
                          AND t5.sys_code = 'SJ'
                          AND t5.dictcode = a.from_stru
                          AND t5.dictlang = 'zh_CN') from_stru_name,
                      a.spec,
                      (SELECT t6.dictvalue
                         FROM dams_gen_dict t6
                        WHERE t6.dicttype = 'SJFILE_SPEC'
                          AND t6.sys_code = 'SJ'
                          AND t6.dictcode = a.spec
                          AND t6.dictlang = 'zh_CN') spec_name,
                      row_number() over(ORDER BY t1.create_time DESC) rn
                 FROM dams_wf_hist_task    t1,
                      dams_wf_hist_process t4, --    申请任务
                      sj_gov_file_app      a
                WHERE --T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                t4.business_id = a.app_id
             AND EXISTS (SELECT '1'
                   FROM dams_wf_process_conf t3
                  WHERE t3.pdid = t4.process_id
                    AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
             AND EXISTS (SELECT 1
                   FROM dams_wf_hist_task t2
                  WHERE t2.ssic_id = i_ssic_id
                    AND t2.procinst_id = t4.procinst_id)
             AND nvl(t4.business_type, 0) =
                nvl(i_biz_type, nvl(t4.business_type, 0))
             AND t1.create_time =
                (SELECT MAX(ttt1.create_time)
                   FROM dams_wf_hist_task ttt1
                  WHERE ttt1.procinst_id = t4.procinst_id)
             AND t4.valid_flag = '1'
                ORDER BY t1.create_time DESC)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_over_task_sj_qry;
  
  PROCEDURE proc_trans_task_sj_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                   i_biz_type  IN VARCHAR2, --业务类型
                                   i_sys_code  IN VARCHAR2, --系统编码
                                   i_beg_num   IN VARCHAR2, --查询开始记录
                                   i_fetch_num IN VARCHAR2, --查询记录数
                                   o_retcode   OUT VARCHAR2, --返回值
                                   o_total_num OUT VARCHAR2, --记录总数
                                   o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                   ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_beg_num || '|' ||
                                    i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TRANS_TASK_QRY';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_process t4, sj_gov_file_app a
     WHERE --T4.SYS_CODE = NVL(I_SYS_CODE, T4.SYS_CODE)
     t4.business_id = a.app_id
     AND EXISTS (SELECT '1'
        FROM dams_wf_process_conf t3
       WHERE t3.pdid = t4.process_id
         AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
     AND EXISTS
     (SELECT '1'
        FROM dams_wf_task t2
       WHERE t2.procinst_id = t4.procinst_id
         AND t2.ssic_id_tran = i_ssic_id
         AND t2.auth_state = 1
         AND t2.task_state IN ('completed'))
     AND nvl(t4.business_type, 0) = nvl(i_biz_type, nvl(t4.business_type, 0))
     AND t4.valid_flag = '1';
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             prev_task_id,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stru_name,
             docno,
             title1,
             from_stru,
             from_stru_name,
             spec,
             spec_name
        FROM (SELECT t1.task_id,
                      t1.task_name,
                      nvl(t1.ssic_id,
                          (SELECT wm_concat(t4.userid_)
                             FROM jbpm4_participation t4
                            WHERE t4.task_ = t1.task_id)) ssic_id,
                      nvl(pckg_dams_util.func_getusernamebyid(t1.ssic_id),
                          (SELECT wm_concat(pckg_dams_util.func_getusernamebyid(t4.userid_))
                             FROM jbpm4_participation t4
                            WHERE t4.task_ = t1.task_id)) user_name,
                      t1.stru_id,
                      t1.role_id,
                      t1.prev_task_id,
                      t1.prev_ssic_id,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                      t1.prev_stru_id,
                      t1.prev_role_id,
                      t4.procinst_id,
                      t4.process_id,
                      t4.process_name,
                      t1.form_id,
                      pckg_dams_util.func_decode('BIZ_TYPE', t4.business_type, 'DAMS') business_type,
                      t4.business_id,
                      t4.title,
                      t1.create_time,
                      t1.take_time,
                      t1.end_time,
                      t1.task_state,
                      t1.outgoing_name,
                      t1.opinion,
                      t4.creator_id applyer,
                      (SELECT u.name
                         FROM dams_user u
                        WHERE u.ssic_id = t4.creator_id) apply_name,
                      to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                      t4.creator_stru app_stru,
                      (SELECT b.stru_sname
                         FROM dams_branch b
                        WHERE b.stru_id = t4.creator_stru) app_stru_name,
                      a.docno,
                      a.title title1,
                      a.from_stru,
                      (SELECT t5.dictvalue
                         FROM dams_gen_dict t5
                        WHERE t5.dicttype = 'SJFILE_FROM_STRU'
                          AND t5.sys_code = 'SJ'
                          AND t5.dictcode = a.from_stru
                          AND t5.dictlang = 'zh_CN') from_stru_name,
                      a.spec,
                      (SELECT t6.dictvalue
                         FROM dams_gen_dict t6
                        WHERE t6.dicttype = 'SJFILE_SPEC'
                          AND t6.sys_code = 'SJ'
                          AND t6.dictcode = a.spec
                          AND t6.dictlang = 'zh_CN') spec_name,
                      row_number() over(ORDER BY t1.create_time DESC) rn
                 FROM dams_wf_task    t1,
                      dams_wf_process t4, --    申请任务
                      sj_gov_file_app a
                WHERE --T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                t1.business_id = a.app_id
             AND EXISTS (SELECT '1'
                   FROM dams_wf_process_conf t3
                  WHERE t3.pdid = t4.process_id
                    AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
             AND t1.procinst_id = t4.procinst_id
             AND EXISTS (SELECT '1'
                   FROM dams_wf_task t2
                  WHERE t2.procinst_id = t1.procinst_id
                    AND t2.ssic_id_tran = i_ssic_id
                    AND t2.auth_state = '1'
                    AND t2.task_state IN ('completed'))
               --AND T1.IS_FIRST = 0
               --AND T1.TASK_STATE IN ('open', 'suspended')
             AND nvl(t4.business_type, 0) =
                nvl(i_biz_type, nvl(t4.business_type, 0))
             AND t1.create_time =
                (SELECT MAX(ttt1.create_time)
                   FROM dams_wf_task ttt1
                  WHERE ttt1.procinst_id = t4.procinst_id)
             AND t4.valid_flag = '1'
                ORDER BY t1.create_time DESC)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_trans_task_sj_qry;
  
  PROCEDURE proc_trash_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                           i_biz_type   IN VARCHAR2, --业务类型
                           i_sys_code   IN VARCHAR2, --系统编码
                           i_condition  IN VARCHAR2, --系统编码或事项
                           i_start_date IN VARCHAR2, --开始时间
                           i_end_date   IN VARCHAR2, --结束时间
                           i_beg_num    IN VARCHAR2, --查询开始记录
                           i_fetch_num  IN VARCHAR2, --查询记录数
                           o_retcode    OUT VARCHAR2, --返回值
                           o_total_num  OUT VARCHAR2, --记录总数
                           o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                           ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                     i_sys_code || '|' || i_beg_num || '|' ||
                                     i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TRASH_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_sav_num    NUMBER;
    v_out_num    NUMBER := 0; --kfzx-guoq03 add;
    v_admin_flag VARCHAR2(1); --kfzx-guoq03 add;
    v_admin_stru VARCHAR2(4000); --KFZX-GUOQ03 ADD;
    v_inner_num  NUMBER; --kfzx-guoq03 add; 内部垃圾数据
    v_sysflag    VARCHAR2(1); --kfzx-guoq03 add 判断是否该系统可应用公文系统方式来恢复文档
    v_condition  VARCHAR2(4000);
    v_min_date   VARCHAR2(10) := '1900-01-01';
    v_end_date   DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_end_date IS NULL OR i_end_date = '' THEN
      NULL;
    ELSE
      v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
    END IF;
    v_min_date := TO_CHAR( ADD_MONTHS( v_end_date, -3 ), 'YYYY-MM-DD'); 
    IF i_sys_code IN ('GW', 'ASMS') OR i_sys_code IS NULL THEN
      v_sysflag := '0';
    END IF;
    v_condition := upper(i_condition);
    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_task t,
           (SELECT t2.task_id,
                   row_number() over(PARTITION BY t2.procinst_id ORDER BY t2.create_time DESC) rn
              FROM dams_wf_process t1, dams_wf_task t2
             WHERE t1.valid_flag = '0'
               AND t1.procinst_id = t2.procinst_id
               AND t2.task_state = 'drop'
               AND t2.ssic_id = i_ssic_id --opentask时插数，废止再open后
               AND t1.business_type = nvl(i_biz_type, t1.business_type)
               AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
               AND t2.create_time >=
                   to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
               AND t2.create_time <= v_end_date
               AND ((upper(t1.business_id) LIKE '%' || v_condition || '%' OR
                   upper(t1.title) LIKE '%' || v_condition || '%') OR
                   i_condition IS NULL)) s
     WHERE t.task_id = s.task_id
       AND s.rn = 1;
    SELECT COUNT(1)
      INTO v_sav_num
      FROM dams_save_info t, dams_wf_process_conf t1
     WHERE t.valid_flag = '0'
       AND t.applyer = i_ssic_id
       AND t.biz_type = nvl(i_biz_type, t.biz_type)
       AND t.pdid = t1.pdid
       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
       AND t.app_time >=
           to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
       AND t.app_time <= v_end_date
       AND ((upper(t.app_id) LIKE '%' || v_condition || '%' OR
           upper(t.info) LIKE '%' || v_condition || '%') OR
           i_condition IS NULL);
    v_inner_num := v_sav_num + o_total_num;
    --kfzx-guoq03 add start 外部公文系统垃圾文档  处理人须为公文系统垃圾文档 处理人须为公文系统管理员其他子系统如果逻辑相同可添加系统号
    SELECT MAX('1'),
           listagg(bc.major_stru_id, ',') within GROUP(ORDER BY ur.stru_id)
      INTO v_admin_flag, v_admin_stru
      FROM dams_user_role_rel ur, dams_role r, dams_branch_catalog bc
     WHERE ur.ssic_id = i_ssic_id
       AND v_sysflag = '0'
       AND ur.sys_code IN ('GW', 'ASMS')
       AND ur.role_id = r.role_id
       AND r.role_type = 'ADMIN'
       AND ur.stru_id = bc.stru_id;
    IF v_admin_flag = '1' THEN
      --是管理员那么和当前人的ssic没关系了只和机构相关
      SELECT COUNT(1)
        INTO v_out_num
        FROM dams_outer_todo_task t
       WHERE v_sysflag = '0' --公文系统的垃圾箱处理方式，是否是公文系统
         AND t.sys_code IN ('GW', 'ASMS')
         AND t.sys_code = nvl(i_sys_code, t.sys_code)
         AND to_date(t.create_time, 'yyyymmddhh24miss') >=
             to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
         AND to_date(t.create_time, 'yyyymmddhh24miss') <= v_end_date
         AND EXISTS
       (SELECT 1
                FROM dams_branch_relation br
               WHERE t.creator_stru = br.bnch_id
                 AND instr(v_admin_stru, br.up_bnch_id) > 0)
         AND t.task_state = 'suspended'
         AND nvl(t.business_type, 0) =
             nvl(i_biz_type, nvl(t.business_type, 0))
         AND ((upper(t.business_id) LIKE '%' || v_condition || '%' OR
             upper(t.title) LIKE '%' || v_condition || '%') OR
             i_condition IS NULL);
    ELSE
      SELECT COUNT(1)
        INTO v_out_num
        FROM dams_outer_todo_task t
       WHERE t.ssic_id = i_ssic_id
         AND v_sysflag = '0' --公文系统的垃圾箱处理方式，是否是公文系统
         AND t.sys_code IN ('GW', 'ASMS')
         AND t.sys_code = nvl(i_sys_code, t.sys_code)
         AND to_date(t.create_time, 'yyyymmddhh24miss') >=
             to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
         AND to_date(t.create_time, 'yyyymmddhh24miss') <= v_end_date
         AND EXISTS
       (SELECT 1
                FROM dams_branch_relation br
               WHERE t.creator_stru = br.bnch_id)
         AND t.task_state = 'suspended'
         AND nvl(t.business_type, 0) =
             nvl(i_biz_type, nvl(t.business_type, 0))
         AND ((upper(t.business_id) LIKE '%' || v_condition || '%' OR
             upper(t.title) LIKE '%' || v_condition || '%') OR
             i_condition IS NULL);
    END IF;
    --kfzx-guoq03 modified O_TOTAL_NUM := O_TOTAL_NUM + V_SAV_NUM;
    o_total_num := o_total_num + v_sav_num + v_out_num;
    IF v_admin_flag = '1' THEN
      OPEN o_list_cur FOR
        SELECT task_id,
               title,
               form_id,
               business_type,
               business_id,
               applyer,
               applyer_name,
               apply_time,
               url,
               url_view,
               sys_code
          FROM (SELECT t.task_id,
                       s.title,
                       t.form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  s.business_type,
                                                  'DAMS') business_type,
                       t.business_id,
                       t.ssic_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.ssic_id) applyer_name,
                       to_char(t.create_time, 'YYYY-MM-DD') apply_time,
                       '' url, --kfzx-guoq03 added
                       '' url_view, --kfzx-guoq03 added
                       t.sys_code,
                       row_number() over(ORDER BY t.create_time DESC) rn
                  FROM dams_wf_task t,
                       (SELECT t2.task_id,
                               t1.business_type,
                               t1.sys_code,
                               t1.title,
                               row_number() over(PARTITION BY t2.procinst_id ORDER BY t2.create_time DESC) rn
                          FROM dams_wf_process t1, dams_wf_task t2
                         WHERE t1.valid_flag = '0'
                           AND t1.procinst_id = t2.procinst_id
                           AND t2.task_state = 'drop'
                           AND t2.ssic_id = i_ssic_id --opentask时插数，废止在open之后，一定有数
                           AND t1.business_type =
                               nvl(i_biz_type, t1.business_type)
                           AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                           AND t2.create_time >=
                               to_date(nvl(i_start_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t2.create_time <= v_end_date
                           AND ((upper(t1.business_id) LIKE
                               '%' || v_condition || '%' OR
                               upper(t1.title) LIKE
                               '%' || v_condition || '%') OR
                               i_condition IS NULL)) s
                 WHERE t.task_id = s.task_id
                   AND s.rn = 1
                UNION ALL
                SELECT 0 task_id,
                       t.info title,
                       func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  t.biz_type,
                                                  'DAMS') business_type,
                       t.app_id business_id,
                       t.applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.applyer) applyer_name,
                       to_char(t.app_time, 'YYYY-MM-DD') apply_time,
                       '' url, --kfzx-guoq03 added
                       '' url_view, --kfzx-guoq03 added
                       t1.sys_code,
                       row_number() over(ORDER BY t.app_time DESC) rn
                  FROM dams_save_info t, dams_wf_process_conf t1
                 WHERE t.valid_flag = '0'
                   AND t.applyer = i_ssic_id
                   AND t.biz_type = nvl(i_biz_type, t.biz_type)
                   AND t.pdid = t1.pdid
                   AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                   AND t.app_time >=
                       to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
                   AND t.app_time <= v_end_date
                   AND ((upper(t.app_id) LIKE '%' || v_condition || '%' OR
                       upper(t.info) LIKE '%' || v_condition || '%') OR
                       i_condition IS NULL)
                --kfzx-guoq03 add for operated the out trash file
                UNION ALL
                SELECT t.task_id task_id,
                       t.title title,
                       decode(v_admin_flag, '1', '', 'false') form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  t.business_type,
                                                  'DAMS') business_type,
                       t.business_id business_id,
                       t.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.ssic_id) applyer_name,
                       to_char(to_date(t.create_time, 'yyyymmddhh24miss'),
                               'YYYY-MM-DD') apply_time,
                       t0.sysurl || '/' || t0.context || '/' || t.url url,
                       t0.sysurl || '/' || t0.context || '/' || t.url_view url_view,
                       t0.sys_code,
                       row_number() over(ORDER BY t.create_time DESC) rn
                  FROM dams_outer_todo_task t, dams_sys_url_cfg t0
                 WHERE v_sysflag = '0'
                   AND t.sys_code = t0.sys_code
                   AND t.sys_code IN ('GW', 'ASMS')
                   AND t.sys_code = nvl(i_sys_code, t.sys_code)
                   AND to_date(t.create_time, 'yyyymmddhh24miss') >=
                       to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
                   AND to_date(t.create_time, 'yyyymmddhh24miss') <=
                       v_end_date
                   AND ((upper(t.business_id) LIKE '%' || v_condition || '%' OR
                       upper(t.title) LIKE '%' || v_condition || '%') OR
                       i_condition IS NULL)
                   AND EXISTS
                 (SELECT 1
                          FROM dams_branch_relation br
                         WHERE t.creator_stru = br.bnch_id
                           AND instr(v_admin_stru,
                                     --查询所属机构下面的所有废止文件 --zhouw04
                                     br.up_bnch_id) > 0)
                   AND t.task_state = 'suspended'
                   AND nvl(t.business_type, 0) =
                       nvl(i_biz_type, nvl(t.business_type, 0)))
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      OPEN o_list_cur FOR
        SELECT task_id,
               title,
               form_id,
               business_type,
               business_id,
               applyer,
               applyer_name,
               apply_time,
               url,
               url_view,
               sys_code
          FROM (SELECT t.task_id,
                       s.title,
                       t.form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE', s.business_type, 'DAMS') business_type,
                       t.business_id,
                       t.ssic_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.ssic_id) applyer_name,
                       to_char(t.create_time, 'YYYY-MM-DD') apply_time,
                       '' url, --kfzx-guoq03 added
                       '' url_view, --kfzx-guoq03 added
                       t.sys_code,
                       row_number() over(ORDER BY t.create_time DESC) rn
                  FROM dams_wf_task t,
                       (SELECT t2.task_id,
                               t1.business_type,
                               t1.sys_code,
                               t1.title,
                               row_number() over(PARTITION BY t2.procinst_id ORDER BY t2.create_time DESC) rn
                          FROM dams_wf_process t1, dams_wf_task t2
                         WHERE t1.valid_flag = '0'
                           AND t1.procinst_id = t2.procinst_id
                           AND t2.task_state = 'drop'
                           AND t2.ssic_id = i_ssic_id --opentask时插数，废止在open之后，一定有数
                           AND t1.business_type =
                               nvl(i_biz_type, t1.business_type)
                           AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                           AND t2.create_time >=
                               to_date(nvl(i_start_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t2.create_time <= v_end_date
                           AND ((upper(t1.business_id) LIKE
                               '%' || v_condition || '%' OR
                               upper(t1.title) LIKE
                               '%' || v_condition || '%') OR
                               i_condition IS NULL)) s
                 WHERE t.task_id = s.task_id
                   AND s.rn = 1
                UNION ALL
                SELECT 0 task_id,
                       t.info title,
                       func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  t.biz_type,
                                                  'DAMS') business_type,
                       t.app_id business_id,
                       t.applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.applyer) applyer_name,
                       to_char(t.app_time, 'YYYY-MM-DD') apply_time,
                       '' url, --kfzx-guoq03 added
                       '' url_view, --kfzx-guoq03 added
                       t1.sys_code,
                       row_number() over(ORDER BY t.app_time DESC) rn
                  FROM dams_save_info t, dams_wf_process_conf t1
                 WHERE t.valid_flag = '0'
                   AND t.applyer = i_ssic_id
                   AND t.biz_type = nvl(i_biz_type, t.biz_type)
                   AND t.pdid = t1.pdid
                   AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                   AND t.app_time >=
                       to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
                   AND t.app_time <= v_end_date
                   AND ((upper(t.app_id) LIKE '%' || v_condition || '%' OR
                       upper(t.info) LIKE '%' || v_condition || '%') OR
                       i_condition IS NULL)
                --kfzx-guoq03 add for operated the out trash file
                UNION ALL
                SELECT t.task_id task_id,
                       t.title title,
                       decode(v_admin_flag, '1', '', 'false') form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  t.business_type,
                                                  'DAMS') business_type,
                       t.business_id business_id,
                       t.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t.ssic_id) applyer_name,
                       to_char(to_date(t.create_time, 'yyyymmddhh24miss'),
                               'YYYY-MM-DD') apply_time,
                       t0.sysurl || '/' || t0.context || '/' || t.url url,
                       t0.sysurl || '/' || t0.context || '/' || t.url_view url_view,
                       t0.sys_code,
                       row_number() over(ORDER BY t.create_time DESC) rn
                  FROM dams_outer_todo_task t, dams_sys_url_cfg t0
                 WHERE t.ssic_id = i_ssic_id
                   AND v_sysflag = '0'
                   AND t.sys_code = t0.sys_code
                   AND t.sys_code IN ('GW', 'ASMS')
                   AND t.sys_code = nvl(i_sys_code, t.sys_code)
                   AND to_date(t.create_time, 'yyyymmddhh24miss') >=
                       to_date(nvl(i_start_date, v_min_date), 'YYYY-MM-DD')
                   AND to_date(t.create_time, 'yyyymmddhh24miss') <=
                       v_end_date
                   AND ((upper(t.business_id) LIKE '%' || v_condition || '%' OR
                       upper(t.title) LIKE '%' || v_condition || '%') OR
                       i_condition IS NULL)
                   AND EXISTS
                 (SELECT 1
                          FROM dams_branch_relation br
                         WHERE t.creator_stru = br.bnch_id)
                   AND t.task_state = 'suspended'
                   AND nvl(t.business_type, 0) =
                       nvl(i_biz_type, nvl(t.business_type, 0)))
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_trash_qry;
  
  PROCEDURE proc_se_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_apply_stru IN VARCHAR2, --申请部门
                             i_reason     IN VARCHAR2, --材料名称与份数
                             i_sec_lv     IN VARCHAR2, --密级
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_applyer || '|' ||
                                     i_apply_stru || '|' || i_reason || '|' ||
                                     i_sec_lv || '|' || i_type || '|' ||
                                     i_beg_num || '|' || i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_SE_TASK_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_qry_type   VARCHAR2(1);
    v_stru_id    dams_branch.stru_id%TYPE;
    v_count      NUMBER;
    v_applyer    VARCHAR2(3000);
    v_apply_stru VARCHAR2(3000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --根据是否模糊查询，指定是否要查询申请人和申请部门
    IF i_type = 0 THEN
      --模糊查询
      v_applyer    := NULL;
      v_apply_stru := NULL;
    ELSE
      v_applyer    := i_applyer;
      v_apply_stru := i_apply_stru;
    END IF;
    --查询是否有管理员权限
    SELECT MAX(t1.stru_id)
      INTO v_stru_id
      FROM dams_user_role_rel t1, dams_prm_biz_admin t2
     WHERE t2.biz_type = 'SE01'
       AND t2.role = t1.role_id
       AND t1.ssic_id = i_ssic_id;
    IF v_stru_id IS NULL THEN
      --无管理员权限，查询个人经手
      v_qry_type := 0;
    ELSE
      --有管理员权限，查机构
      v_qry_type := 1;
    END IF;
    --查询权限内的流程数据
    DELETE FROM dams_tmp_task_id;
    v_count := pckg_dams_task.func_get_task_id(i_ssic_id,
                                               v_stru_id,
                                               'SE',
                                               '',
                                               v_applyer,
                                               v_apply_stru,
                                               '',
                                               '',
                                               '',
                                               v_qry_type);

    IF i_type = 0 THEN
      --模糊查询

      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id     t1,
             se_print_app         t2,
             viw_dams_wf_all_task t3,
             dams_gen_dict        t4,
             dams_branch          t5,
             dams_user            t6
       WHERE t1.business_id = t2.app_id
         AND t1.task_id = t3.task_id
         AND t4.sys_code = 'SE'
         AND t4.dictlang = 'zh_CN'
         AND t4.dicttype = 'SEC_LV'
         AND t4.dictcode = t2.sec_lv
         AND t5.stru_id = t2.app_stru
         AND t6.ssic_id = t2.applyer
         AND (t6.name LIKE '%' || i_qry_str || '%' OR
             t5.stru_fname LIKE '%' || i_qry_str || '%' OR
             t2.print_reason LIKE '%' || i_qry_str || '%' OR
             t4.dictvalue LIKE '%' || i_qry_str || '%');
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.info title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t2.info,
                       t3.task_name,
                       to_char(t2.app_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t2.app_time DESC) rn
                  FROM dams_tmp_task_id     t1,
                       se_print_app         t2,
                       viw_dams_wf_all_task t3,
                       dams_gen_dict        t4,
                       dams_branch          t5,
                       dams_user            t6
                 WHERE t1.business_id = t2.app_id
                   AND t1.task_id = t3.task_id
                   AND t4.sys_code = 'SE'
                   AND t4.dictlang = 'zh_CN'
                   AND t4.dicttype = 'SEC_LV'
                   AND t4.dictcode = t2.sec_lv
                   AND t5.stru_id = t2.app_stru
                   AND t6.ssic_id = t2.applyer
                   AND (t6.name LIKE '%' || i_qry_str || '%' OR
                       t5.stru_fname LIKE '%' || i_qry_str || '%' OR
                       t2.print_reason LIKE '%' || i_qry_str || '%' OR
                       t4.dictvalue LIKE '%' || i_qry_str || '%')) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      --高级查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1, se_print_app t2, viw_dams_wf_all_task t3
       WHERE t1.business_id = t2.app_id
         AND t1.task_id = t3.task_id
         AND t2.sec_lv = nvl(i_sec_lv, t2.sec_lv)
         AND t2.print_reason LIKE '%' || i_reason || '%';
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.info title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE',
                                          t.business_type,
                                          'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t2.info,
                       t3.task_name,
                       to_char(t2.app_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t2.app_time DESC) rn
                  FROM dams_tmp_task_id     t1,
                       se_print_app         t2,
                       viw_dams_wf_all_task t3
                 WHERE t1.business_id = t2.app_id
                   AND t1.task_id = t3.task_id
                   AND t2.sec_lv = nvl(i_sec_lv, t2.sec_lv)
                   AND t2.print_reason LIKE '%' || i_reason || '%') t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_se_task_qry;
  
  PROCEDURE proc_sf_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_apply_stru IN VARCHAR2, --申请部门
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --证卡使用人
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_applyer || '|' ||
                                     i_apply_stru || '|' || i_biz_type || '|' ||
                                     i_receiver || '|' || i_type || '|' ||
                                     i_beg_num || '|' || i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_SF_TASK_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_qry_type   VARCHAR2(1);
    v_stru_id    dams_branch.stru_id%TYPE;
    v_count      NUMBER;
    v_applyer    VARCHAR2(3000);
    v_apply_stru VARCHAR2(3000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --根据是否模糊查询，指定是否要查询申请人和申请部门
    IF i_type = 0 THEN
      --模糊查询
      v_applyer    := NULL;
      v_apply_stru := NULL;
    ELSE
      v_applyer    := i_applyer;
      v_apply_stru := i_apply_stru;
    END IF;
    --查询是否有管理员权限
    SELECT MAX(t1.stru_id)
      INTO v_stru_id
      FROM dams_user_role_rel t1, dams_prm_biz_admin t2
     WHERE t2.biz_type IN ('SF01', 'SF02', 'SF03', 'SF04', 'SF05', 'SF06')
       AND t2.role = t1.role_id
       AND t1.ssic_id = i_ssic_id;
    IF v_stru_id IS NULL THEN
      --无管理员权限，查询个人经手
      v_qry_type := 0;
    ELSE
      --有管理员权限，查机构
      v_qry_type := 1;
    END IF;
    --查询权限内的流程数据
    DELETE FROM dams_tmp_task_id;
    v_count := pckg_dams_task.func_get_task_id(i_ssic_id,
                                               v_stru_id,
                                               'SF',
                                               '',
                                               v_applyer,
                                               v_apply_stru,
                                               '',
                                               '',
                                               '',
                                               v_qry_type);

    IF i_type = 0 THEN
      --模糊查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id     t1,
             viw_dams_wf_all_task t3,
             dams_gen_dict        t4,
             dams_branch          t5,
             dams_user            t6
       WHERE t1.task_id = t3.task_id
         AND t4.sys_code = 'DAMS'
         AND t4.dictlang = 'zh_CN'
         AND t4.dicttype = 'BIZ_TYPE'
         AND t4.dictcode = t1.business_type
         AND t5.stru_id = t1.creator_stru
         AND t6.ssic_id = t1.creator_id
         AND (t6.name LIKE '%' || i_qry_str || '%' OR
             t5.stru_fname LIKE '%' || i_qry_str || '%' OR
             t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS --证卡领取人的条件
              (SELECT 1
                 FROM sf_card_new_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type IN ('SF01', 'SF02')) OR EXISTS
              (SELECT 1
                 FROM sf_card_chg_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type IN ('SF03', 'SF04')));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id     t1,
                       viw_dams_wf_all_task t3,
                       dams_gen_dict        t4,
                       dams_branch          t5,
                       dams_user            t6
                 WHERE t1.task_id = t3.task_id
                   AND t4.sys_code = 'DAMS'
                   AND t4.dictlang = 'zh_CN'
                   AND t4.dicttype = 'BIZ_TYPE'
                   AND t4.dictcode = t1.business_type
                   AND t5.stru_id = t1.creator_stru
                   AND t6.ssic_id = t1.creator_id
                   AND (t6.name LIKE '%' || i_qry_str || '%' OR
                       t5.stru_fname LIKE '%' || i_qry_str || '%' OR
                       t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS --证卡领取人的条件
                        (SELECT 1
                           FROM sf_card_new_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type IN ('SF01', 'SF02')) OR
                        EXISTS
                        (SELECT 1
                           FROM sf_card_chg_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type IN ('SF03', 'SF04')))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      --高级查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
       WHERE t1.task_id = t3.task_id
         AND t1.business_type = nvl(i_biz_type, t1.business_type)
         AND (i_receiver IS NULL OR EXISTS --证卡领取人的条件
              (SELECT 1
                 FROM sf_card_new_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type IN ('SF01', 'SF02')) OR EXISTS
              (SELECT 1
                 FROM sf_card_chg_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type IN ('SF03', 'SF04')));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
                 WHERE t1.task_id = t3.task_id
                   AND t1.business_type = nvl(i_biz_type, t1.business_type)
                   AND (i_receiver IS NULL OR EXISTS --证卡领取人的条件
                        (SELECT 1
                           FROM sf_card_new_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type IN ('SF01', 'SF02')) OR
                        EXISTS
                        (SELECT 1
                           FROM sf_card_chg_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type IN ('SF03', 'SF04')))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_sf_task_qry;
  
  PROCEDURE proc_sn_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_dev_type   IN VARCHAR2, --设备类型
                             i_close_flag IN VARCHAR2, --流转状态
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                     i_receiver || '|' || i_depart_id || '|' ||
                                     i_dev_type || '|' || i_close_flag || '|' ||
                                     i_start_date || '|' || i_end_date || '|' ||
                                     i_type || '|' || i_qry_str || '|' ||
                                     i_beg_num || '|' || i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_SN_TASK_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_qry_type   VARCHAR2(1);
    v_stru_id    VARCHAR2(32767);
    v_count      NUMBER;
    v_count2     NUMBER;
    v_applyer    VARCHAR2(3000);
    v_apply_stru VARCHAR2(3000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --经办人
    v_applyer    := NULL;
    v_apply_stru := NULL;
    BEGIN
      --查询是否有管理员权限
      SELECT COUNT(1)
        INTO v_count2
        FROM dams_user_role_rel t
       WHERE t.ssic_id = i_ssic_id
         AND t.role_id IN ('SN001', 'SN004');
      IF v_count2 > 0 THEN
        --SN001 本部运行处库房管理员
        --SN004 本部运行处设备管理员
        --可以查看所有申请
        v_stru_id := '0010100500';
      ELSE
        v_stru_id := NULL;
      END IF;
    EXCEPTION
      WHEN no_data_found THEN
        v_stru_id := NULL;
      WHEN OTHERS THEN
        v_stru_id := NULL;
    END;
    IF v_stru_id IS NULL THEN
      --无管理员权限，查询个人经手
      v_qry_type := 0;
    ELSE
      --有管理员权限，查机构
      v_qry_type := 1;
    END IF;
    --查询权限内的流程数据
    DELETE FROM dams_tmp_task_id;
    v_count := pckg_dams_task.func_get_task_id(i_ssic_id,
                                               v_stru_id,
                                               'SN',
                                               '',
                                               v_applyer,
                                               v_apply_stru,
                                               i_start_date,
                                               i_end_date,
                                               '',
                                               v_qry_type);
    IF i_type = 0 THEN
      --模糊查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1,
             viw_dams_wf_all_task t3,
             (SELECT t.app_id,
                     t.inner_seq,
                     t.user_name,
                     pckg_dams_util.func_get_stru_name(t.dept) depart_name,
                     pckg_dams_util.func_get_stru_name(t.office) office_name,
                     t.dev_type,
                     t.dev_id,
                     t.dev_serial
                FROM sa_dev_wh_apply_det t
               WHERE t.inner_seq = '1'
              UNION ALL
              SELECT t.app_id,
                     t.inner_seq,
                     t.user_name,
                     pckg_dams_util.func_get_stru_name(t.depart) depart_name,
                     pckg_dams_util.func_get_stru_name(t.office) office_name,
                     t.dev_type,
                     t.dev_id,
                     t.dev_serial
                FROM sa_dev_scrap_apply_det t
               WHERE t.inner_seq = '1') td
       WHERE t1.task_id = t3.task_id
         AND t1.business_type IN ('SN01', 'SN02')
         AND t1.business_id = td.app_id
         AND (td.user_name LIKE '%' || i_qry_str || '%' OR
             td.depart_name LIKE '%' || i_qry_str || '%' OR
             td.office_name LIKE '%' || i_qry_str || '%');
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id,
               t.user_name,
               t.depart_name,
               t.office_name,
               pckg_dams_util.func_decode('DEV_NAME', t.dev_type, 'SO') dev_type,
               t.dev_id,
               t.dev_serial
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       td.user_name,
                       td.depart_name,
                       td.office_name,
                       td.dev_type,
                       td.dev_id,
                       td.dev_serial,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id t1,
                       viw_dams_wf_all_task t3,
                       (SELECT t.app_id,
                               t.inner_seq,
                               t.user_name,
                               pckg_dams_util.func_get_stru_name(t.dept) depart_name,
                               pckg_dams_util.func_get_stru_name(t.office) office_name,
                               t.dev_type,
                               t.dev_id,
                               t.dev_serial
                          FROM sa_dev_wh_apply_det t
                         WHERE t.inner_seq = '1'
                        UNION ALL
                        SELECT t.app_id,
                               t.inner_seq,
                               t.user_name,
                               pckg_dams_util.func_get_stru_name(t.depart) depart_name,
                               pckg_dams_util.func_get_stru_name(t.office) office_name,
                               t.dev_type,
                               t.dev_id,
                               t.dev_serial
                          FROM sa_dev_scrap_apply_det t
                         WHERE t.inner_seq = '1') td
                 WHERE t1.task_id = t3.task_id
                   AND t1.business_type IN ('SN01', 'SN02')
                   AND t1.business_id = td.app_id
                   AND (td.user_name LIKE '%' || i_qry_str || '%' OR
                       td.depart_name LIKE '%' || i_qry_str || '%' OR
                       td.office_name LIKE '%' || i_qry_str || '%')) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      --高级查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1,
             viw_dams_wf_all_task t3,
             (SELECT t.app_id,
                     t.inner_seq,
                     t.user_name,
                     t.dept depart,
                     pckg_dams_util.func_get_stru_name(t.dept) depart_name,
                     t.office,
                     pckg_dams_util.func_get_stru_name(t.office) office_name,
                     t.dev_type,
                     pckg_dams_util.func_decode('DEV_NAME', t.dev_type, 'SO') dev_type_name,
                     t.dev_id,
                     t.dev_serial
                FROM sa_dev_wh_apply_det t
               WHERE t.inner_seq = '1'
              UNION ALL
              SELECT t.app_id,
                     t.inner_seq,
                     t.user_name,
                     t.depart,
                     pckg_dams_util.func_get_stru_name(t.depart) depart_name,
                     t.office,
                     pckg_dams_util.func_get_stru_name(t.office) office_name,
                     t.dev_type,
                     pckg_dams_util.func_decode('DEV_NAME', t.dev_type, 'SO') dev_type_name,
                     t.dev_id,
                     t.dev_serial
                FROM sa_dev_scrap_apply_det t
               WHERE t.inner_seq = '1') td
       WHERE t1.task_id = t3.task_id
         AND t1.business_type IN ('SN01', 'SN02')
         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
         AND t1.business_id = td.app_id
         AND t1.business_type = nvl(i_biz_type, t1.business_type)
         AND td.user_name LIKE '%' || i_receiver || '%'
         AND td.dev_type = nvl(i_dev_type, td.dev_type)
         AND (i_depart_id IS NULL OR EXISTS
              (SELECT 1
                 FROM dams_branch_relation br
                WHERE br.bnch_id = td.depart
                  AND br.up_bnch_id = i_depart_id));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id,
               t.user_name,
               t.depart_name,
               t.office_name,
               t.dev_type_name,
               t.dev_id,
               t.dev_serial
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       td.user_name,
                       td.depart_name,
                       td.office_name,
                       td.dev_type_name,
                       td.dev_id,
                       td.dev_serial,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id t1,
                       viw_dams_wf_all_task t3,
                       (SELECT t.app_id,
                               t.inner_seq,
                               t.user_name,
                               t.dept depart,
                               pckg_dams_util.func_get_stru_name(t.dept) depart_name,
                               t.office,
                               pckg_dams_util.func_get_stru_name(t.office) office_name,
                               t.dev_type,
                               pckg_dams_util.func_decode('DEV_NAME', t.dev_type, 'SO') dev_type_name,
                               t.dev_id,
                               t.dev_serial
                          FROM sa_dev_wh_apply_det t
                         WHERE t.inner_seq = '1'
                        UNION ALL
                        SELECT t.app_id,
                               t.inner_seq,
                               t.user_name,
                               t.depart,
                               pckg_dams_util.func_get_stru_name(t.depart) depart_name,
                               t.office,
                               pckg_dams_util.func_get_stru_name(t.office) office_name,
                               t.dev_type,
                               pckg_dams_util.func_decode('DEV_NAME', t.dev_type, 'SO') dev_type_name,
                               t.dev_id,
                               t.dev_serial
                          FROM sa_dev_scrap_apply_det t
                         WHERE t.inner_seq = '1') td
                 WHERE t1.task_id = t3.task_id
                   AND t1.business_type IN ('SN01', 'SN02')
                   AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                   AND t1.business_id = td.app_id
                   AND t1.business_type = nvl(i_biz_type, t1.business_type)
                   AND td.user_name LIKE '%' || i_receiver || '%'
                   AND td.dev_type = nvl(i_dev_type, td.dev_type)
                   AND (i_depart_id IS NULL OR EXISTS
                        (SELECT 1
                           FROM dams_branch_relation br
                          WHERE br.bnch_id = td.depart
                            AND br.up_bnch_id = i_depart_id))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_sn_task_qry;
  
  PROCEDURE proc_so_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_app_id     IN VARCHAR2, --核实单号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_office_id  IN VARCHAR2, --使用人处室
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_app_id || '|' ||
                                     i_applyer || '|' || i_biz_type || '|' ||
                                     i_receiver || '|' || i_depart_id || '|' ||
                                     i_office_id || '|' || i_start_date || '|' ||
                                     i_end_date || '|' || i_type || '|' ||
                                     i_qry_str || '|' || i_beg_num || '|' ||
                                     i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_SO_TASK_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_qry_type   VARCHAR2(1);
    v_stru_id    VARCHAR2(32767);
    v_count      NUMBER;
    v_count2     NUMBER;
    v_applyer    VARCHAR2(3000);
    v_apply_stru VARCHAR2(3000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --根据是否模糊查询，指定是否要查询申请人和申请部门
    IF i_type = 0 THEN
      --模糊查询
      v_applyer    := NULL;
      v_apply_stru := NULL;
    ELSE
      v_applyer    := i_applyer;
      v_apply_stru := NULL;
    END IF;
    BEGIN
      --查询是否有管理员权限
      SELECT COUNT(1)
        INTO v_count2
        FROM dams_user_role_rel t
       WHERE t.ssic_id = i_ssic_id
         AND t.role_id IN ('SO004', 'SO099');
      IF v_count2 > 0 THEN
        --本部运行处设备管理员、子系统管理员 可以查看所有部室申请
        v_stru_id := '0010100500';
      ELSE
        --设备管理员可以查看自己部室申请
        --SELECT dams_conn(t1.stru_id)
        SELECT t1.stru_id
          INTO v_stru_id
          FROM dams_user_role_rel t1
         WHERE t1.ssic_id = i_ssic_id
           AND t1.role_id = 'SO006';
      END IF;
    EXCEPTION
      WHEN no_data_found THEN
        v_stru_id := NULL;
      WHEN OTHERS THEN
        v_stru_id := NULL;
    END;
    IF v_stru_id IS NULL THEN
      --无管理员权限，查询个人经手
      v_qry_type := 0;
    ELSE
      --有管理员权限，查机构
      v_qry_type := 1;
    END IF;
    --查询权限内的流程数据
    DELETE FROM dams_tmp_task_id;
    v_count := pckg_dams_task.func_get_task_id(i_ssic_id,
                                               v_stru_id,
                                               'SO',
                                               '',
                                               v_applyer,
                                               v_apply_stru,
                                               i_start_date,
                                               i_end_date,
                                               '',
                                               v_qry_type);
    IF i_type = 0 THEN
      --模糊查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id     t1,
             viw_dams_wf_all_task t3,
             dams_gen_dict        t4,
             dams_branch          t5,
             dams_user            t6
       WHERE t1.task_id = t3.task_id
         AND t4.sys_code = 'DAMS'
         AND t4.dictlang = 'zh_CN'
         AND t4.dicttype = 'BIZ_TYPE'
         AND t4.dictcode = t1.business_type
         AND t5.stru_id = t1.creator_stru
         AND t6.ssic_id = t1.creator_id
         AND (t3.business_id LIKE '%' || i_qry_str || '%' OR
             t6.name LIKE '%' || i_qry_str || '%' OR
             t5.stru_fname LIKE '%' || i_qry_str || '%' OR
             t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS
              (SELECT 1
                 FROM sa_dev_apply_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_qry_str || '%'
                  AND t1.business_type = 'SO01'));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id     t1,
                       viw_dams_wf_all_task t3,
                       dams_gen_dict        t4,
                       dams_branch          t5,
                       dams_user            t6
                 WHERE t1.task_id = t3.task_id
                   AND t4.sys_code = 'DAMS'
                   AND t4.dictlang = 'zh_CN'
                   AND t4.dicttype = 'BIZ_TYPE'
                   AND t4.dictcode = t1.business_type
                   AND t5.stru_id = t1.creator_stru
                   AND t6.ssic_id = t1.creator_id
                   AND (t3.business_id LIKE '%' || i_qry_str || '%' OR
                       t6.name LIKE '%' || i_qry_str || '%' OR
                       t5.stru_fname LIKE '%' || i_qry_str || '%' OR
                       t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS
                        (SELECT 1
                           FROM sa_dev_apply_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_qry_str || '%'
                            AND t1.business_type = 'SO01'))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      --高级查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
       WHERE t1.task_id = t3.task_id
         AND t1.business_type = nvl(i_biz_type, t1.business_type)
         AND t1.business_id = nvl(i_app_id, t1.business_id)
         AND (i_receiver IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_dev_apply_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type = 'SO01'))
         AND (i_depart_id IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_dev_apply_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type = 'SO01'
                  AND tt1.user_dept = br.bnch_id
                  AND br.up_bnch_id = i_depart_id))
         AND (i_office_id IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_dev_apply_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type = 'SO01'
                  AND tt1.user_office = br.bnch_id
                  AND br.up_bnch_id = i_office_id));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
                 WHERE t1.task_id = t3.task_id
                   AND t1.business_type = nvl(i_biz_type, t1.business_type)
                   AND t1.business_id = nvl(i_app_id, t1.business_id)
                   AND (i_receiver IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_dev_apply_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type = 'SO01'))
                   AND (i_depart_id IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_dev_apply_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type = 'SO01'
                            AND tt1.user_dept = br.bnch_id
                            AND br.up_bnch_id = i_depart_id))
                   AND (i_office_id IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_dev_apply_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type = 'SO01'
                            AND tt1.user_office = br.bnch_id
                            AND br.up_bnch_id = i_office_id))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_so_task_qry;
  
  PROCEDURE proc_sp_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_app_id     IN VARCHAR2, --核实单号
                             i_applyer    IN VARCHAR2, --申请人名字
                             i_biz_type   IN VARCHAR2, -- 申请类型
                             i_receiver   IN VARCHAR2, --设备使用人
                             i_depart_id  IN VARCHAR2, --使用人部室
                             i_office_id  IN VARCHAR2, --使用人处室
                             i_start_date IN VARCHAR2, --开始时间
                             i_end_date   IN VARCHAR2, --结束时间
                             i_type       IN VARCHAR2, --0-模糊查询，1-高级查询
                             i_qry_str    IN VARCHAR2, --模糊查询参数
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_app_id || '|' ||
                                     i_applyer || '|' || i_biz_type || '|' ||
                                     i_receiver || '|' || i_depart_id || '|' ||
                                     i_office_id || '|' || i_start_date || '|' ||
                                     i_end_date || '|' || i_type || '|' ||
                                     i_qry_str || '|' || i_beg_num || '|' ||
                                     i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_SP_TASK_QRY';
    v_step       db_log.step_no%TYPE := 0;
    v_qry_type   VARCHAR2(1);
    v_stru_id    VARCHAR2(32767);
    v_count      NUMBER;
    v_count2     NUMBER;
    v_applyer    VARCHAR2(3000);
    v_apply_stru VARCHAR2(3000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    --根据是否模糊查询，指定是否要查询申请人和申请部门
    IF i_type = 0 THEN
      --模糊查询
      v_applyer    := NULL;
      v_apply_stru := NULL;
    ELSE
      v_applyer    := i_applyer;
      v_apply_stru := NULL;
    END IF;
    BEGIN
      --查询是否有管理员权限
      SELECT COUNT(1)
        INTO v_count2
        FROM dams_user_role_rel t
       WHERE t.ssic_id = i_ssic_id
         AND t.role_id IN ('SP005', 'SP099');
      IF v_count2 > 0 THEN
        --本部运行处耗材库管理员、子系统管理员 可以查看所有部室申请
        v_stru_id := '0010100500';
      ELSE
        --设备管理员可以查看自己部室申请
        --SELECT dams_conn(t1.stru_id)
        SELECT t1.stru_id
          INTO v_stru_id
          FROM dams_user_role_rel t1
         WHERE t1.ssic_id = i_ssic_id
           AND t1.role_id = 'SP007';
      END IF;
    EXCEPTION
      WHEN no_data_found THEN
        v_stru_id := NULL;
      WHEN OTHERS THEN
        v_stru_id := NULL;
    END;
    IF v_stru_id IS NULL THEN
      --无管理员权限，查询个人经手
      v_qry_type := 0;
    ELSE
      --有管理员权限，查机构
      v_qry_type := 1;
    END IF;
    --查询权限内的流程数据
    DELETE FROM dams_tmp_task_id;
    v_count := pckg_dams_task.func_get_task_id(i_ssic_id,
                                               v_stru_id,
                                               'SP',
                                               '',
                                               v_applyer,
                                               v_apply_stru,
                                               i_start_date,
                                               i_end_date,
                                               '',
                                               v_qry_type);
    IF i_type = 0 THEN
      --模糊查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id     t1,
             viw_dams_wf_all_task t3,
             dams_gen_dict        t4,
             dams_branch          t5,
             dams_user            t6
       WHERE t1.task_id = t3.task_id
         AND t4.sys_code = 'DAMS'
         AND t4.dictlang = 'zh_CN'
         AND t4.dicttype = 'BIZ_TYPE'
         AND t4.dictcode = t1.business_type
         AND t5.stru_id = t1.creator_stru
         AND t6.ssic_id = t1.creator_id
         AND (t3.business_id LIKE '%' || i_qry_str || '%' OR
             t6.name LIKE '%' || i_qry_str || '%' OR
             t5.stru_fname LIKE '%' || i_qry_str || '%' OR
             t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS
              (SELECT 1
                 FROM sa_hc_apply_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_qry_str || '%'
                  AND t1.business_type IN ('SP01', 'SP02')) OR EXISTS
              (SELECT 1
                 FROM sa_hct_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.user_name LIKE '%' || i_qry_str || '%'
                  AND t1.business_type = 'SP03'));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id     t1,
                       viw_dams_wf_all_task t3,
                       dams_gen_dict        t4,
                       dams_branch          t5,
                       dams_user            t6
                 WHERE t1.task_id = t3.task_id
                   AND t4.sys_code = 'DAMS'
                   AND t4.dictlang = 'zh_CN'
                   AND t4.dicttype = 'BIZ_TYPE'
                   AND t4.dictcode = t1.business_type
                   AND t5.stru_id = t1.creator_stru
                   AND t6.ssic_id = t1.creator_id
                   AND (t3.business_id LIKE '%' || i_qry_str || '%' OR
                       t6.name LIKE '%' || i_qry_str || '%' OR
                       t5.stru_fname LIKE '%' || i_qry_str || '%' OR
                       t4.dictvalue LIKE '%' || i_qry_str || '%' OR EXISTS
                        (SELECT 1
                           FROM sa_hc_apply_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_qry_str || '%'
                            AND t1.business_type IN ('SP01', 'SP02')) OR
                        EXISTS
                        (SELECT 1
                           FROM sa_hct_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.user_name LIKE '%' || i_qry_str || '%'
                            AND t1.business_type = 'SP03'))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSE
      --高级查询
      --查询总数
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
       WHERE t1.task_id = t3.task_id
         AND t1.business_type = nvl(i_biz_type, t1.business_type)
         AND t1.business_id = nvl(i_app_id, t1.business_id)
         AND (i_receiver IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_hc_apply_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.receiver LIKE '%' || i_receiver || '%'
                  AND t1.business_type IN ('SP01', 'SP02')) OR EXISTS
              (SELECT 1
                 FROM sa_hct_app_det tt1
                WHERE tt1.app_id = t1.business_id
                  AND tt1.user_name LIKE '%' || i_receiver || '%'
                  AND t1.business_type = 'SP03'))
         AND (i_depart_id IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_hc_apply_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type IN ('SP01', 'SP02')
                  AND tt1.user_dept = br.bnch_id
                  AND br.up_bnch_id = i_depart_id) OR EXISTS
              (SELECT 1
                 FROM sa_hct_app_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type = 'SP03'
                  AND tt1.user_dept = br.bnch_id
                  AND br.up_bnch_id = i_depart_id))
         AND (i_office_id IS NULL OR EXISTS
              (SELECT 1
                 FROM sa_hc_apply_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type IN ('SP01', 'SP02')
                  AND tt1.user_office = br.bnch_id
                  AND br.up_bnch_id = i_office_id) OR EXISTS
              (SELECT 1
                 FROM sa_hct_app_det tt1, dams_branch_relation br
                WHERE tt1.app_id = t1.business_id
                  AND t1.business_type = 'SP03'
                  AND tt1.user_office = br.bnch_id
                  AND br.up_bnch_id = i_office_id));
      --查询游标
      OPEN o_list_cur FOR
        SELECT t.business_id,
               t.title,
               decode(close_flag, 1, '流程结束', t.task_name) task_name,
               decode(close_flag, 1, '', func_get_cur_dealer(t.business_id)) dealer,
               t.apply_time,
               pckg_dams_util.func_decode('BIZ_TYPE', t.business_type, 'DAMS') biz_type,
               t.task_id
          FROM (SELECT t3.business_id,
                       t1.title,
                       t3.task_name,
                       to_char(t1.start_time, 'YYYY-MM-DD') apply_time,
                       t3.close_flag,
                       t1.business_type,
                       t1.task_id,
                       row_number() over(ORDER BY t1.start_time DESC) rn
                  FROM dams_tmp_task_id t1, viw_dams_wf_all_task t3
                 WHERE t1.task_id = t3.task_id
                   AND t1.business_type = nvl(i_biz_type, t1.business_type)
                   AND t1.business_id = nvl(i_app_id, t1.business_id)
                   AND (i_receiver IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_hc_apply_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.receiver LIKE '%' || i_receiver || '%'
                            AND t1.business_type IN ('SP01', 'SP02')) OR
                        EXISTS
                        (SELECT 1
                           FROM sa_hct_app_det tt1
                          WHERE tt1.app_id = t1.business_id
                            AND tt1.user_name LIKE '%' || i_receiver || '%'
                            AND t1.business_type = 'SP03'))
                   AND (i_depart_id IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_hc_apply_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type IN ('SP01', 'SP02')
                            AND tt1.user_dept = br.bnch_id
                            AND br.up_bnch_id = i_depart_id) OR EXISTS
                        (SELECT 1
                           FROM sa_hct_app_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type = 'SP03'
                            AND tt1.user_dept = br.bnch_id
                            AND br.up_bnch_id = i_depart_id))
                   AND (i_office_id IS NULL OR EXISTS
                        (SELECT 1
                           FROM sa_hc_apply_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type IN ('SP01', 'SP02')
                            AND tt1.user_office = br.bnch_id
                            AND br.up_bnch_id = i_office_id) OR EXISTS
                        (SELECT 1
                           FROM sa_hct_app_det tt1, dams_branch_relation br
                          WHERE tt1.app_id = t1.business_id
                            AND t1.business_type = 'SP03'
                            AND tt1.user_office = br.bnch_id
                            AND br.up_bnch_id = i_office_id))) t
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_sp_task_qry;
  
  PROCEDURE proc_st_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, --业务类型
                             i_sys_code   IN VARCHAR2, --系统编码
                             i_applyer    IN VARCHAR2, --申请人
                             i_begin_date IN VARCHAR2, --查询开始时间
                             i_end_date   IN VARCHAR2, --查询结束时间
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param            db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                           i_sys_code || '|' || i_beg_num || '|' ||
                                           i_fetch_num;
    v_proc_name        db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_ST_TASK_QRY';
    v_step             db_log.step_no%TYPE := 0;
    v_sav_num          NUMBER;
    v_admin_strus      VARCHAR2(1000);
    v_min_date         VARCHAR2(10) := '1900-01-01';
    v_end_date         DATE := SYSDATE;
    v_dsmfile_biz_type VARCHAR2(1000) := 'ST03,ST04,ST05,ST06,ST07,ST08,ST09,ST10'; --涉密载体流程
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_end_date IS NULL OR i_end_date = '' THEN
      NULL;
    ELSE
      v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
    END IF;
    --获取保密密码工作人员所在机构(非保密密码工作人员为空)
    --SELECT dams_conn(t.stru_id)
    SELECT t.stru_id
      INTO v_admin_strus
      FROM dams_user_role_rel t
     WHERE t.ssic_id = i_ssic_id
       AND t.role_id = 'DR20000029'
       AND t.auth_state IN ('0', '2');
    --计算待办数量，保密密码工作人员可查看涉密载体相关流程所有下辖待办
    SELECT COUNT(1)
      INTO o_total_num
      FROM viw_dams_wf_all_task t1, viw_dams_wf_all_process t4
     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
       AND ((EXISTS (SELECT '1'
                       FROM jbpm4_participation t2
                      WHERE t2.userid_ = i_ssic_id
                        AND t2.task_ = t1.task_id) AND
            (v_admin_strus IS NULL OR v_admin_strus IS NOT NULL
            --AND instr(v_dsmfile_biz_type, t4.business_type) = 0
            )) OR (EXISTS (SELECT '1'
                              FROM dams_branch_relation br
                             WHERE br.bnch_id = t4.creator_stru
                               AND instr(v_admin_strus, br.up_bnch_id) > 0) AND
            v_admin_strus IS NOT NULL
           --AND instr(v_dsmfile_biz_type, t4.business_type) > 0
           ))
       AND t1.procinst_id = t4.procinst_id
          --AND t1.task_state IN ('open', 'suspended')
       AND nvl(t4.business_type, 0) =
           nvl(i_biz_type, nvl(t4.business_type, 0))
       AND t4.valid_flag = '1'
       AND EXISTS (SELECT '1'
              FROM viw_dams_wf_all_task t2, dams_user u
             WHERE t2.procinst_id = t1.procinst_id
               AND t2.ssic_id = u.ssic_id
               AND u.name = nvl(i_applyer, u.name)
               AND t2.task_state IN ('completed'))
       AND EXISTS
     (SELECT '1'
              FROM viw_dams_wf_all_process t5
             WHERE t5.procinst_id = t1.procinst_id
               AND t5.start_time >=
                   to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
               AND t5.start_time <= v_end_date);
    --草稿数量
    SELECT COUNT(1)
      INTO v_sav_num
      FROM dams_save_info t,
           (SELECT t2.pdid, MAX(t2.sys_code) sys_code
              FROM dams_wf_process_conf t2
             GROUP BY t2.pdid) t1
     WHERE t.applyer = i_ssic_id
       AND t1.pdid = t.pdid
       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
       AND nvl(t.biz_type, 0) = nvl(i_biz_type, nvl(t.biz_type, 0))
       AND t.valid_flag = 1;
    o_total_num := o_total_num + v_sav_num;
    OPEN o_list_cur FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             role_id,
             prev_task_id,
             prev_task_name,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stur_name,
             cfg_id,
             url, --kfzx-guoq03 add
             show_business_id,
             rn
        FROM (SELECT task_id,
                     task_name,
                     user_name,
                     stru_id,
                     role_id,
                     prev_task_id,
                     prev_task_name,
                     prev_ssic_id,
                     prev_person_name,
                     prev_stru_id,
                     procinst_id,
                     process_id,
                     process_name,
                     form_id,
                     business_type,
                     business_id,
                     title,
                     create_time,
                     take_time,
                     end_time,
                     task_state,
                     outgoing_name,
                     opinion,
                     apply_name,
                     apply_time,
                     app_stru,
                     app_stur_name,
                     cfg_id,
                     url,
                     business_id show_business_id,
                     row_number() over(ORDER BY tn.ordertime DESC) rn
                FROM (SELECT 0 task_id,
                             '拟稿' task_name,
                             t.applyer,
                             (SELECT u.name
                                FROM dams_user u
                               WHERE u.ssic_id = t.applyer) user_name,
                             t.app_stru stru_id,
                             '' role_id,
                             0 prev_task_id,
                             '拟稿' prev_task_name,
                             t.applyer prev_ssic_id,
                             (SELECT u.name
                                FROM dams_user u
                               WHERE u.ssic_id = t.applyer) prev_person_name,
                             '' prev_stru_id,
                             '' prev_role_id,
                             t.app_id procinst_id,
                             '' process_id,
                             '' process_name,
                             func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
                             t.biz_type business_type,
                             t.app_id business_id,
                             t.info title,
                             to_char(t.app_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
                             to_char(t.app_time, 'YYYY-MM-DD') take_time,
                             '' end_time,
                             'draft' task_state,
                             '' outgoing_name,
                             '' opinion,
                             t.applyer applyer,
                             (SELECT u.name
                                FROM dams_user u
                               WHERE u.ssic_id = t.applyer) apply_name,
                             to_char(t.app_time, 'YYYY-MM-DD') apply_time,
                             t.app_stru,
                             (SELECT b.stru_sname
                                FROM dams_branch b
                               WHERE b.stru_id = t.app_stru) app_stur_name,
                             NULL cfg_id,
                             '' url,
                             to_char(t.app_time, 'yyyymmddhh24miss') ordertime,
                             '' out_sys_seq
                        FROM dams_save_info t,
                             (SELECT t2.pdid, MAX(t2.sys_code) sys_code
                                FROM dams_wf_process_conf t2
                               GROUP BY t2.pdid) t1
                       WHERE t.applyer = i_ssic_id
                         AND t1.pdid = t.pdid
                         AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
                         AND nvl(t.biz_type, 0) =
                             nvl(i_biz_type, nvl(t.biz_type, 0))
                         AND t.valid_flag = 1
                      UNION ALL (SELECT t1.task_id,
                                       t1.task_name,
                                       t1.ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.ssic_id) user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       (SELECT wt.task_name
                                          FROM viw_dams_wf_all_task wt
                                         WHERE wt.task_id = t1.prev_task_id) prev_task_name,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       to_char(t1.create_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
                                       to_char(t1.take_time, 'YYYY-MM-DD') take_time,
                                       to_char(t1.end_time, 'YYYY-MM-DD') end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       t4.cfg_id cfgid,
                                       '' url,
                                       to_char(t1.create_time, 'yyyymmddhh24miss') ordertime,
                                       '' out_sys_seq
                                  FROM viw_dams_wf_all_task    t1,
                                       viw_dams_wf_all_process t4
                                 WHERE t4.sys_code =
                                       nvl(i_sys_code, t4.sys_code)
                                   AND ((EXISTS
                                        (SELECT '1'
                                            FROM jbpm4_participation t2
                                           WHERE t2.userid_ = i_ssic_id
                                             AND t2.task_ = t1.task_id) AND (v_admin_strus IS NULL OR
                                        v_admin_strus IS NOT NULL
                                        --AND instr(v_dsmfile_biz_type,t4.business_type) = 0
                                        )) OR
                                       (EXISTS
                                        (SELECT '1'
                                            FROM dams_branch_relation br
                                           WHERE br.bnch_id = t4.creator_stru
                                             AND instr(v_admin_strus,
                                                       br.up_bnch_id) > 0) AND
                                        v_admin_strus IS NOT NULL
                                       --AND instr(v_dsmfile_biz_type, t4.business_type) > 0
                                       ))
                                   AND t1.procinst_id = t4.procinst_id
                                      --AND T1.IS_FIRST = 1
                                      --AND t1.task_state IN ('open', 'suspended')
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1'
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM viw_dams_wf_all_task t2,
                                               dams_user            u
                                         WHERE t2.procinst_id = t1.procinst_id
                                           AND t2.ssic_id = u.ssic_id
                                           AND u.name = nvl(i_applyer, u.name)
                                           AND t2.task_state IN ('completed'))
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM viw_dams_wf_all_process t5
                                         WHERE t5.procinst_id = t1.procinst_id
                                           AND t5.start_time >=
                                               to_date(nvl(i_begin_date,
                                                           v_min_date),
                                                       'YYYY-MM-DD')
                                           AND t5.start_time <= v_end_date))) tn)
       WHERE rn >= to_number(i_beg_num)
         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_st_task_qry;
  
  PROCEDURE proc_sx_task_qry(i_ssic_id       IN VARCHAR2, --统一认证号
                             i_biz_type      IN VARCHAR2, --业务类型
                             i_sys_code      IN VARCHAR2, --系统编码
                             i_close_flag    IN VARCHAR2, --流程结束标志
                             i_applyer       IN VARCHAR2, --申请人
                             i_begin_date    IN VARCHAR2, --查询开始时间
                             i_end_date      IN VARCHAR2, --查询结束时间
                             i_beg_num       IN VARCHAR2, --查询开始记录
                             i_fetch_num     IN VARCHAR2, --查询记录数
                             i_city          IN VARCHAR2,
                             i_origin_app_id IN VARCHAR2,
                             o_retcode       OUT VARCHAR2, --返回值
                             o_total_num     OUT VARCHAR2, --记录总数
                             o_list_cur      OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param        db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                       i_sys_code || '|' || i_close_flag || '|' ||
                                       i_applyer || '|' || i_begin_date || '|' ||
                                       i_end_date || '|' || i_beg_num || '|' ||
                                       i_fetch_num;
    v_proc_name    db_log.proc_name%TYPE := 'PCKG_DAMS_TASK_BAK.PROC_SX_TASK_QRY';
    v_step         db_log.step_no%TYPE := 0;
    v_min_date     VARCHAR2(10) := '1900-01-01';
    v_end_date     DATE := SYSDATE;
    v_num          NUMBER := 0;
    v_num2         NUMBER := 0;
    v_status_code  VARCHAR2(10) := '1';
    v_dept_stru_id dams_branch_catalog.dept_stru_id%TYPE;
    v_sys_code     dams_sys_conf.sys_code%TYPE;
  BEGIN
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);

    o_retcode := '0';

    IF i_end_date IS NULL OR i_end_date = '' THEN
      NULL;
    ELSE
      v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
    END IF;

    v_sys_code := nvl(i_sys_code, 'SX');

    IF i_biz_type = 'SX08' THEN
      -- 如果拥有财务处角色，就全部查出来
      SELECT COUNT(1)
        INTO v_num
        FROM dams_user_role_rel
       WHERE ssic_id = i_ssic_id
         AND role_id IN ('SX011', 'SX012', 'SX013', 'SX099')
         AND auth_state IN ('0', '1', '2');

      SELECT COUNT(1)
        INTO v_num2
        FROM dams_user_role_rel
       WHERE ssic_id = i_ssic_id
         AND role_id = 'SX016'
         AND auth_state IN ('0', '1', '2');
      IF v_num > 0 THEN
        v_status_code := 'SX0899';
      ELSIF v_num2 > 0 THEN
        v_status_code := 'SX0816';
      ELSE
        v_status_code := 'SX0801';
      END IF;
    ELSIF i_biz_type = 'SX05' OR i_biz_type = 'SX06' THEN
      v_status_code := '99';
    ELSE
      SELECT COUNT(1)
        INTO v_num
        FROM dams_user_role_rel t1, dams_prm_biz_admin t2
       WHERE t1.ssic_id = i_ssic_id
         AND t1.sys_code = v_sys_code
         AND t1.role_id = t2.role;

      IF v_num > 0 THEN
        --是ADMIN
        v_status_code := '2';
      ELSE
        v_status_code := '1';
      END IF;
    END IF;
    IF v_status_code = 'SX0899' THEN
      -- 财务处人员
      SELECT COUNT(1)
        INTO o_total_num
        FROM sx_pay_busi_trip_ext t, dams_user u
       WHERE t.is_valid = '1'
         AND t.trip_staff_id = u.ssic_id(+)
         AND (t.trip_staff_name = i_applyer OR u.name = i_applyer OR
             i_applyer IS NULL)
         AND t.update_time >=
             to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
         AND t.update_time <= v_end_date
         AND (i_city IS NULL OR EXISTS
              (SELECT 1
                 FROM sx_pay_busi_trip_ext_city c
                WHERE t.ext_id = c.ext_id
                  AND c.city_name LIKE '%' || i_city || '%'))
         AND (i_origin_app_id IS NULL OR t.ori_app_id = i_origin_app_id);
      OPEN o_list_cur FOR
        SELECT '' task_id,
               '' task_name,
               '' ssic_id,
               '' user_name,
               '' stru_id,
               '' prev_task_id,
               '' prev_ssic_id,
               '' prev_person_name,
               '' prev_stru_id,
               '' procinst_id,
               '' process_id,
               '' process_name,
               '' form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', i_biz_type, 'DAMS') business_type,
               tmp.ext_id business_id,
               (SELECT title
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) title,
               '' create_time,
               '' take_time,
               '' end_time,
               '' task_state,
               '' outgoing_name,
               'INNER' opinion,
               '' applyer,
               to_char(tmp.update_time, 'YYYY-MM-DD') apply_time,
               '' complete_time,
               '' close_flag,
               (SELECT url
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) url
          FROM (SELECT t.ext_id,
                       t.update_time,
                       row_number() over(ORDER BY t.update_time DESC) rn
                  FROM sx_pay_busi_trip_ext t, dams_user u
                 WHERE t.is_valid = '1'
                   AND t.trip_staff_id = u.ssic_id(+)
                   AND (t.trip_staff_name = i_applyer OR u.name = i_applyer OR
                       i_applyer IS NULL)
                   AND t.update_time >=
                       to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                   AND t.update_time <= v_end_date
                   AND (i_city IS NULL OR EXISTS
                        (SELECT 1
                           FROM sx_pay_busi_trip_ext_city c
                          WHERE t.ext_id = c.ext_id
                            AND c.city_name LIKE '%' || i_city || '%'))
                   AND (i_origin_app_id IS NULL OR
                       t.ori_app_id = i_origin_app_id)) tmp
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = 'SX0816' THEN
      --部门管理员
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT 1
                FROM sx_pay_busi_trip_ext t
               WHERE t.is_valid = '1'
                 AND t.trip_staff_id = i_ssic_id
                 AND t.update_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t.update_time <= v_end_date
                 AND (i_city IS NULL OR EXISTS
                      (SELECT 1
                         FROM sx_pay_busi_trip_ext_city c
                        WHERE t.ext_id = c.ext_id
                          AND c.city_name LIKE '%' || i_city || '%'))
                 AND (i_origin_app_id IS NULL OR
                     t.ori_app_id = i_origin_app_id)
              UNION ALL
              SELECT 1
                FROM sx_pay_busi_trip_ext t,
                     dams_user u,
                     (SELECT br.up_bnch_id AS stru_id
                        FROM dams_branch_relation br, dams_user_role_rel ur
                       WHERE br.bnch_id = ur.stru_id
                         AND br.up_bnch_man_grade = '2'
                         AND ur.ssic_id = i_ssic_id
                         AND ur.role_id = 'SX016'
                         AND ur.auth_state IN ('0', '1', '2')) sr
               WHERE t.is_valid = '1'
                 AND t.dept_stru = sr.stru_id
                 AND t.trip_staff_id = u.ssic_id(+)
                 AND t.trip_staff_id != i_ssic_id
                 AND (t.trip_staff_name = i_applyer OR u.name = i_applyer OR
                     i_applyer IS NULL)
                 AND t.update_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t.update_time <= v_end_date
                 AND (i_city IS NULL OR EXISTS
                      (SELECT 1
                         FROM sx_pay_busi_trip_ext_city c
                        WHERE t.ext_id = c.ext_id
                          AND c.city_name LIKE '%' || i_city || '%'))
                 AND (i_origin_app_id IS NULL OR
                     t.ori_app_id = i_origin_app_id));
      OPEN o_list_cur FOR
        SELECT '' task_id,
               '' task_name,
               '' ssic_id,
               '' user_name,
               '' stru_id,
               '' prev_task_id,
               '' prev_ssic_id,
               '' prev_person_name,
               '' prev_stru_id,
               '' procinst_id,
               '' process_id,
               '' process_name,
               '' form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', i_biz_type, 'DAMS') business_type,
               tmp.ext_id business_id,
               (SELECT title
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) title,
               '' create_time,
               '' take_time,
               '' end_time,
               '' task_state,
               '' outgoing_name,
               'INNER' opinion,
               '' applyer,
               to_char(tmp.update_time, 'YYYY-MM-DD') apply_time,
               '' complete_time,
               '' close_flag,
               (SELECT url
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) url
          FROM (SELECT bte.ext_id,
                       bte.update_time,
                       row_number() over(ORDER BY bte.update_time DESC) rn
                  FROM (SELECT t.ext_id, t.update_time
                          FROM sx_pay_busi_trip_ext t
                         WHERE t.is_valid = '1'
                           AND t.trip_staff_id = i_ssic_id
                           AND t.update_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t.update_time <= v_end_date
                           AND (i_city IS NULL OR EXISTS
                                (SELECT 1
                                   FROM sx_pay_busi_trip_ext_city c
                                  WHERE t.ext_id = c.ext_id
                                    AND c.city_name LIKE '%' || i_city || '%'))
                           AND (i_origin_app_id IS NULL OR
                               t.ori_app_id = i_origin_app_id)
                        UNION ALL
                        SELECT t.ext_id, t.update_time
                          FROM sx_pay_busi_trip_ext t,
                               dams_user u,
                               (SELECT br.up_bnch_id AS stru_id
                                  FROM dams_branch_relation br,
                                       dams_user_role_rel   ur
                                 WHERE br.bnch_id = ur.stru_id
                                   AND br.up_bnch_man_grade = '2'
                                   AND ur.ssic_id = i_ssic_id
                                   AND ur.role_id = 'SX016'
                                   AND ur.auth_state IN ('0', '1', '2')) sr
                         WHERE t.is_valid = '1'
                           AND t.dept_stru = sr.stru_id
                           AND t.trip_staff_id = u.ssic_id(+)
                           AND t.trip_staff_id != i_ssic_id
                           AND (t.trip_staff_name = i_applyer OR
                               u.name = i_applyer OR i_applyer IS NULL)
                           AND t.update_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t.update_time <= v_end_date
                           AND (i_city IS NULL OR EXISTS
                                (SELECT 1
                                   FROM sx_pay_busi_trip_ext_city c
                                  WHERE t.ext_id = c.ext_id
                                    AND c.city_name LIKE '%' || i_city || '%'))
                           AND (i_origin_app_id IS NULL OR
                               t.ori_app_id = i_origin_app_id)) bte) tmp
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = 'SX0801' THEN
      -- 非财务处人员
      SELECT COUNT(1)
        INTO o_total_num
        FROM sx_pay_busi_trip_ext t
       WHERE t.is_valid = '1'
         AND t.trip_staff_id = i_ssic_id
         AND t.update_time >=
             to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
         AND t.update_time <= v_end_date
         AND (i_city IS NULL OR EXISTS
              (SELECT 1
                 FROM sx_pay_busi_trip_ext_city c
                WHERE t.ext_id = c.ext_id
                  AND c.city_name LIKE '%' || i_city || '%'))
         AND (i_origin_app_id IS NULL OR t.ori_app_id = i_origin_app_id);
      OPEN o_list_cur FOR
        SELECT '' task_id,
               '' task_name,
               '' ssic_id,
               '' user_name,
               '' stru_id,
               '' prev_task_id,
               '' prev_ssic_id,
               '' prev_person_name,
               '' prev_stru_id,
               '' procinst_id,
               '' process_id,
               '' process_name,
               '' form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', i_biz_type, 'DAMS') business_type,
               tmp.ext_id business_id,
               (SELECT title
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) title,
               '' create_time,
               '' take_time,
               '' end_time,
               '' task_state,
               '' outgoing_name,
               'INNER' opinion,
               '' applyer,
               to_char(tmp.update_time, 'YYYY-MM-DD') apply_time,
               '' complete_time,
               '' close_flag,
               (SELECT url
                  FROM viw_dams_inner_all_task a
                 WHERE a.business_id = tmp.ext_id
                   AND rownum = 1) url
          FROM (SELECT t.ext_id,
                       t.update_time,
                       row_number() over(ORDER BY t.update_time DESC) rn
                  FROM sx_pay_busi_trip_ext t
                 WHERE t.is_valid = '1'
                   AND t.trip_staff_id = i_ssic_id
                   AND t.update_time >=
                       to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                   AND t.update_time <= v_end_date
                   AND (i_city IS NULL OR EXISTS
                        (SELECT 1
                           FROM sx_pay_busi_trip_ext_city c
                          WHERE t.ext_id = c.ext_id
                            AND c.city_name LIKE '%' || i_city || '%'))
                   AND (i_origin_app_id IS NULL OR
                       t.ori_app_id = i_origin_app_id)) tmp
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

    ELSIF v_status_code = '99' THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM (SELECT t.business_id
                FROM dams_inner_todo_task t
               WHERE t.task_state = 'open'
                 AND t.sys_code = nvl(i_sys_code, t.sys_code)
                 AND t.business_type = i_biz_type
                 AND (t.ssic_id = i_ssic_id OR t.creator_id = i_ssic_id)
                 AND t.create_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t.create_time <= v_end_date
              UNION ALL
              SELECT t.business_id
                FROM dams_inner_over_task t
               WHERE t.task_state = 'completed'
                 AND t.sys_code = nvl(i_sys_code, t.sys_code)
                 AND t.business_type = i_biz_type
                 AND (t.ssic_id = i_ssic_id OR t.creator_id = i_ssic_id)
                 AND t.create_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t.create_time <= v_end_date);

      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               sx.ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = sx.ssic_id) user_name,
               stru_id,
               prev_task_id,
               sx.prev_ssic_id,
               (SELECT u.name
                  FROM dams_user u
                 WHERE u.ssic_id = sx.prev_ssic_id) prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               'INNER' opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag,
               url
          FROM (SELECT task_id,
                       task_name,
                       ssic_id,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  business_type,
                                                  'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       applyer,
                       apply_time,
                       complete_time,
                       close_flag,
                       url,
                       row_number() over(ORDER BY create_time DESC) rn
                  FROM (SELECT t.task_id,
                               NULL task_name,
                               t.ssic_id,
                               NULL stru_id,
                               NULL role_id,
                               NULL prev_task_id,
                               t.prev_ssic_id,
                               NULL prev_stru_id,
                               NULL prev_role_id,
                               NULL procinst_id,
                               NULL process_id,
                               NULL process_name,
                               NULL form_id,
                               t.business_type,
                               t.business_id,
                               t.title,
                               t.create_time,
                               t.take_time,
                               t.end_time,
                               t.task_state,
                               NULL outgoing_name,
                               NULL opinion,
                               t.creator_id applyer,
                               to_char(t.create_time, 'YYYY-MM-DD') apply_time,
                               NULL complete_time,
                               NULL close_flag,
                               t.url
                          FROM dams_inner_todo_task t
                         WHERE t.task_state = 'open'
                           AND t.sys_code = nvl(i_sys_code, t.sys_code)
                           AND t.business_type = i_biz_type
                           AND (t.ssic_id = i_ssic_id OR
                               t.creator_id = i_ssic_id)
                           AND t.create_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t.create_time <= v_end_date
                        UNION ALL
                        SELECT t.task_id,
                               NULL task_name,
                               t.ssic_id,
                               NULL stru_id,
                               NULL role_id,
                               NULL prev_task_id,
                               t.prev_ssic_id,
                               NULL prev_stru_id,
                               NULL prev_role_id,
                               NULL procinst_id,
                               NULL process_id,
                               NULL process_name,
                               NULL form_id,
                               t.business_type,
                               t.business_id,
                               t.title,
                               t.create_time,
                               t.take_time,
                               t.end_time,
                               t.task_state,
                               NULL outgoing_name,
                               NULL opinion,
                               t.creator_id applyer,
                               to_char(t.create_time, 'YYYY-MM-DD') apply_time,
                               NULL complete_time,
                               NULL close_flag,
                               t.url
                          FROM dams_inner_over_task t
                         WHERE t.task_state = 'completed'
                           AND t.sys_code = nvl(i_sys_code, t.sys_code)
                           AND t.business_type = i_biz_type
                           AND (t.ssic_id = i_ssic_id OR
                               t.creator_id = i_ssic_id)
                           AND t.create_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t.create_time <= v_end_date)) sx
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;

    IF i_applyer IS NULL THEN
      --i_applyer为空
      IF v_status_code = '2' THEN
        --流程内，是biz_type ADMIN
        SELECT COUNT(1)
          INTO o_total_num
          FROM (SELECT t3.procinst_id
                  FROM viw_dams_wf_all_task t1, viw_dams_wf_all_process t3
                 WHERE t1.procinst_id = t3.procinst_id
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t4
                         WHERE t4.pdid = t3.process_id
                           AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                   AND t3.business_type = nvl(i_biz_type, t3.business_type)
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id));

        OPEN o_list_cur FOR
          SELECT task_id,
                 task_name,
                 ssic_id,
                 decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
                 stru_id,
                 prev_task_id,
                 prev_ssic_id,
                 (SELECT u.name
                    FROM dams_user u
                   WHERE u.ssic_id = prev_ssic_id) prev_person_name,
                 prev_stru_id,
                 procinst_id,
                 process_id,
                 process_name,
                 form_id,
                 business_type,
                 business_id,
                 title,
                 create_time,
                 take_time,
                 end_time,
                 task_state,
                 outgoing_name,
                 opinion,
                 applyer,
                 apply_time,
                 complete_time,
                 close_flag,
                 '' url
            FROM (SELECT t1.task_id,
                         t1.task_name,
                         t1.ssic_id,
                         t1.stru_id,
                         t1.role_id,
                         t1.prev_task_id,
                         t1.prev_ssic_id,
                         t1.prev_stru_id,
                         t1.prev_role_id,
                         t3.procinst_id,
                         t3.process_id,
                         t3.process_name,
                         t1.form_id,
                         pckg_dams_util.func_decode('BIZ_TYPE',
                                                    t3.business_type,
                                                    'DAMS') business_type,
                         t3.business_id,
                         t3.title,
                         t1.create_time,
                         t1.take_time,
                         t1.end_time,
                         t1.task_state,
                         t1.outgoing_name,
                         t1.opinion,
                         t3.creator_id applyer,
                         to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                         t3.complete_time,
                         t3.close_flag,
                         row_number() over(ORDER BY create_time DESC) rn
                    FROM viw_dams_wf_all_task t1, viw_dams_wf_all_process t3
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id))
           WHERE rn >= to_number(i_beg_num)
             AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

      ELSIF v_status_code = '1' THEN

        SELECT COUNT(1)
          INTO v_num
          FROM dams_user_role_rel ur
         WHERE ur.ssic_id = i_ssic_id
           AND ur.role_id = 'SX016';
        IF v_num > 0 THEN
          --部室负责人可以查询本部门所有财务申请单
          SELECT bc.dept_stru_id
            INTO v_dept_stru_id
            FROM dams_branch_catalog bc, dams_user_role_rel ur
           WHERE bc.stru_id = ur.stru_id
             AND ur.ssic_id = i_ssic_id
             AND ur.role_id = 'SX016';

          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task    t1,
                         viw_dams_wf_all_process t3,
                         dams_branch_catalog     bc
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.creator_stru = bc.stru_id
                     AND bc.dept_stru_id = v_dept_stru_id
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id)
                  UNION
                  SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task     t1,
                         viw_dams_wf_all_process  t3,
                         sx_pay_app_busi_trip_det sx
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type = nvl(i_biz_type, 'SX01')
                     AND sx.business_id = t1.business_id
                     AND sx.ssic_id = i_ssic_id
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id));

          OPEN o_list_cur FOR
            SELECT task_id,
                   task_name,
                   ssic_id,
                   decode(close_flag,
                          1,
                          '',
                          func_get_cur_dealer(business_id)) user_name,
                   stru_id,
                   prev_task_id,
                   prev_ssic_id,
                   (SELECT u.name
                      FROM dams_user u
                     WHERE u.ssic_id = prev_ssic_id) prev_person_name,
                   prev_stru_id,
                   procinst_id,
                   process_id,
                   process_name,
                   form_id,
                   pckg_dams_util.func_decode('BIZ_TYPE',
                                              business_type,
                                              'DAMS') business_type,
                   business_id,
                   title,
                   create_time,
                   take_time,
                   end_time,
                   task_state,
                   outgoing_name,
                   opinion,
                   applyer,
                   apply_time,
                   complete_time,
                   close_flag,
                   '' url
              FROM (SELECT task_id,
                           task_name,
                           ssic_id,
                           stru_id,
                           prev_task_id,
                           prev_ssic_id,
                           prev_stru_id,
                           procinst_id,
                           process_id,
                           process_name,
                           form_id,
                           pckg_dams_util.func_decode('BIZ_TYPE',
                                                      business_type,
                                                      'DAMS') business_type,
                           business_id,
                           title,
                           create_time,
                           take_time,
                           end_time,
                           task_state,
                           outgoing_name,
                           opinion,
                           applyer,
                           apply_time,
                           complete_time,
                           close_flag,
                           row_number() over(ORDER BY create_time DESC) rn
                      FROM (SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task    t1,
                                   viw_dams_wf_all_process t3,
                                   dams_branch_catalog     bc
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t4
                                     WHERE t4.pdid = t3.process_id
                                       AND t4.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND t3.creator_stru = bc.stru_id
                               AND bc.dept_stru_id = v_dept_stru_id
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)
                            UNION
                            SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task     t1,
                                   viw_dams_wf_all_process  t3,
                                   sx_pay_app_busi_trip_det sx
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t4
                                     WHERE t4.pdid = t3.process_id
                                       AND t4.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type = nvl(i_biz_type, 'SX01')
                               AND sx.business_id = t1.business_id
                               AND sx.ssic_id = i_ssic_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)))
             WHERE rn >= to_number(i_beg_num)
               AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

        ELSE
          --一般人员
          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task t1, viw_dams_wf_all_process t3
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_task t2
                           WHERE t2.procinst_id = t3.procinst_id
                             AND t2.ssic_id = i_ssic_id)
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id)
                  UNION
                  SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task     t1,
                         viw_dams_wf_all_process  t3,
                         sx_pay_app_busi_trip_det sx
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND sx.business_id = t1.business_id
                     AND sx.ssic_id = i_ssic_id
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id));

          OPEN o_list_cur FOR
            SELECT task_id,
                   task_name,
                   ssic_id,
                   decode(close_flag,
                          1,
                          '',
                          func_get_cur_dealer(business_id)) user_name,
                   stru_id,
                   prev_task_id,
                   prev_ssic_id,
                   (SELECT u.name
                      FROM dams_user u
                     WHERE u.ssic_id = prev_ssic_id) prev_person_name,
                   prev_stru_id,
                   procinst_id,
                   process_id,
                   process_name,
                   form_id,
                   pckg_dams_util.func_decode('BIZ_TYPE',
                                              business_type,
                                              'DAMS') business_type,
                   business_id,
                   title,
                   create_time,
                   take_time,
                   end_time,
                   task_state,
                   outgoing_name,
                   opinion,
                   applyer,
                   apply_time,
                   complete_time,
                   close_flag,
                   '' url
              FROM (SELECT task_id,
                           task_name,
                           ssic_id,
                           stru_id,
                           prev_task_id,
                           prev_ssic_id,
                           prev_stru_id,
                           procinst_id,
                           process_id,
                           process_name,
                           form_id,
                           pckg_dams_util.func_decode('BIZ_TYPE',
                                                      business_type,
                                                      'DAMS') business_type,
                           business_id,
                           title,
                           create_time,
                           take_time,
                           end_time,
                           task_state,
                           outgoing_name,
                           opinion,
                           applyer,
                           apply_time,
                           complete_time,
                           close_flag,
                           row_number() over(ORDER BY create_time DESC) rn
                      FROM (SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task    t1,
                                   viw_dams_wf_all_process t3
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t9
                                     WHERE t9.pdid = t3.process_id
                                       AND t9.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_task t2
                                     WHERE t2.procinst_id = t3.procinst_id
                                       AND t2.ssic_id = i_ssic_id)
                               AND t1.close_flag =
                                   nvl(i_close_flag, t1.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)
                            UNION
                            SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task     t1,
                                   viw_dams_wf_all_process  t3,
                                   sx_pay_app_busi_trip_det sx
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t4
                                     WHERE t4.pdid = t3.process_id
                                       AND t4.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                               AND sx.business_id = t1.business_id
                               AND sx.ssic_id = i_ssic_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)))
             WHERE rn >= to_number(i_beg_num)
               AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

        END IF;
      END IF;
    ELSE
      --i_applyer不为空时
      IF v_status_code = '2' THEN
        --是biz_type ADMIN
        SELECT COUNT(1)
          INTO o_total_num
          FROM (SELECT t3.procinst_id
                  FROM viw_dams_wf_all_task    t1,
                       viw_dams_wf_all_process t3,
                       dams_user               t4
                 WHERE t1.procinst_id = t3.procinst_id
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t4
                         WHERE t4.pdid = t3.process_id
                           AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                   AND t3.business_type = nvl(i_biz_type, t3.business_type)
                      --AND t3.creator_id = t4.ssic_id
                   AND EXISTS (SELECT '1'
                          FROM viw_dams_wf_all_task t2
                         WHERE t2.procinst_id = t3.procinst_id
                           AND t2.ssic_id = t4.ssic_id)
                   AND t4.name = i_applyer
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id)
                UNION
                SELECT t3.procinst_id
                  FROM viw_dams_wf_all_task     t1,
                       viw_dams_wf_all_process  t3,
                       dams_user                t4,
                       sx_pay_app_busi_trip_det sx
                 WHERE t1.procinst_id = t3.procinst_id
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t4
                         WHERE t4.pdid = t3.process_id
                           AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                   AND t3.business_type = nvl(i_biz_type, t3.business_type)
                   AND sx.business_id = t1.business_id
                   AND sx.ssic_id = t4.ssic_id
                   AND t4.name = i_applyer
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id));

        OPEN o_list_cur FOR
          SELECT task_id,
                 task_name,
                 ssic_id,
                 decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
                 stru_id,
                 prev_task_id,
                 prev_ssic_id,
                 (SELECT u.name
                    FROM dams_user u
                   WHERE u.ssic_id = prev_ssic_id) prev_person_name,
                 prev_stru_id,
                 procinst_id,
                 process_id,
                 process_name,
                 form_id,
                 business_type,
                 business_id,
                 title,
                 create_time,
                 take_time,
                 end_time,
                 task_state,
                 outgoing_name,
                 opinion,
                 applyer,
                 apply_time,
                 complete_time,
                 close_flag,
                 '' url
            FROM (SELECT task_id,
                         task_name,
                         ssic_id,
                         stru_id,
                         prev_task_id,
                         prev_ssic_id,
                         prev_stru_id,
                         procinst_id,
                         process_id,
                         process_name,
                         form_id,
                         pckg_dams_util.func_decode('BIZ_TYPE',
                                                    business_type,
                                                    'DAMS') business_type,
                         business_id,
                         title,
                         create_time,
                         take_time,
                         end_time,
                         task_state,
                         outgoing_name,
                         opinion,
                         applyer,
                         apply_time,
                         complete_time,
                         close_flag,
                         row_number() over(ORDER BY create_time DESC) rn
                    FROM (SELECT t1.task_id,
                                 t1.task_name,
                                 t1.ssic_id,
                                 t1.stru_id,
                                 t1.role_id,
                                 t1.prev_task_id,
                                 t1.prev_ssic_id,
                                 t1.prev_stru_id,
                                 t1.prev_role_id,
                                 t3.procinst_id,
                                 t3.process_id,
                                 t3.process_name,
                                 t1.form_id,
                                 t3.business_type,
                                 t3.business_id,
                                 t3.title,
                                 t1.create_time,
                                 t1.take_time,
                                 t1.end_time,
                                 t1.task_state,
                                 t1.outgoing_name,
                                 t1.opinion,
                                 t3.creator_id applyer,
                                 to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                 t3.complete_time,
                                 t3.close_flag
                            FROM viw_dams_wf_all_task    t1,
                                 viw_dams_wf_all_process t3,
                                 dams_user               t4
                           WHERE t1.procinst_id = t3.procinst_id
                             AND EXISTS
                           (SELECT '1'
                                    FROM dams_wf_process_conf t4
                                   WHERE t4.pdid = t3.process_id
                                     AND t4.sys_code =
                                         nvl(i_sys_code, t3.sys_code))
                             AND t3.close_flag =
                                 nvl(i_close_flag, t3.close_flag)
                             AND t3.business_type =
                                 nvl(i_biz_type, t3.business_type)
                                --AND t3.creator_id = t4.ssic_id
                             AND EXISTS
                           (SELECT '1'
                                    FROM viw_dams_wf_all_task t2
                                   WHERE t2.procinst_id = t3.procinst_id
                                     AND t2.ssic_id = t4.ssic_id)
                             AND t4.name = i_applyer
                             AND EXISTS
                           (SELECT '1'
                                    FROM viw_dams_wf_all_process t5
                                   WHERE t5.procinst_id = t3.procinst_id
                                     AND t5.start_time >=
                                         to_date(nvl(i_begin_date, v_min_date),
                                                 'YYYY-MM-DD')
                                     AND t5.start_time <= v_end_date)
                             AND t1.create_time =
                                 (SELECT MAX(tt1.create_time)
                                    FROM viw_dams_wf_all_task tt1
                                   WHERE tt1.procinst_id = t3.procinst_id)
                          UNION
                          SELECT t1.task_id,
                                 t1.task_name,
                                 t1.ssic_id,
                                 t1.stru_id,
                                 t1.role_id,
                                 t1.prev_task_id,
                                 t1.prev_ssic_id,
                                 t1.prev_stru_id,
                                 t1.prev_role_id,
                                 t3.procinst_id,
                                 t3.process_id,
                                 t3.process_name,
                                 t1.form_id,
                                 t3.business_type,
                                 t3.business_id,
                                 t3.title,
                                 t1.create_time,
                                 t1.take_time,
                                 t1.end_time,
                                 t1.task_state,
                                 t1.outgoing_name,
                                 t1.opinion,
                                 t3.creator_id applyer,
                                 to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                 t3.complete_time,
                                 t3.close_flag
                            FROM viw_dams_wf_all_task     t1,
                                 viw_dams_wf_all_process  t3,
                                 dams_user                t4,
                                 sx_pay_app_busi_trip_det sx
                           WHERE t1.procinst_id = t3.procinst_id
                             AND EXISTS
                           (SELECT '1'
                                    FROM dams_wf_process_conf t4
                                   WHERE t4.pdid = t3.process_id
                                     AND t4.sys_code =
                                         nvl(i_sys_code, t3.sys_code))
                             AND t3.close_flag =
                                 nvl(i_close_flag, t3.close_flag)
                             AND t3.business_type =
                                 nvl(i_biz_type, t3.business_type)
                             AND sx.business_id = t1.business_id
                             AND sx.ssic_id = t4.ssic_id
                             AND t4.name = i_applyer
                             AND EXISTS
                           (SELECT '1'
                                    FROM viw_dams_wf_all_process t5
                                   WHERE t5.procinst_id = t3.procinst_id
                                     AND t5.start_time >=
                                         to_date(nvl(i_begin_date, v_min_date),
                                                 'YYYY-MM-DD')
                                     AND t5.start_time <= v_end_date)
                             AND t1.create_time =
                                 (SELECT MAX(tt1.create_time)
                                    FROM viw_dams_wf_all_task tt1
                                   WHERE tt1.procinst_id = t3.procinst_id)) ttt)
           WHERE rn >= to_number(i_beg_num)
             AND rn < to_number(i_beg_num) + to_number(i_fetch_num);

      ELSIF v_status_code = '1' THEN
        SELECT COUNT(1)
          INTO v_num
          FROM dams_user_role_rel ur
         WHERE ur.ssic_id = i_ssic_id
           AND ur.role_id = 'SX016';
        IF v_num > 0 THEN
          --部室负责人可以查询本部门所有财务申请单
          SELECT bc.dept_stru_id
            INTO v_dept_stru_id
            FROM dams_branch_catalog bc, dams_user_role_rel ur
           WHERE bc.stru_id = ur.stru_id
             AND ur.ssic_id = i_ssic_id
             AND ur.role_id = 'SX016';

          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task    t1,
                         viw_dams_wf_all_process t3,
                         dams_branch_catalog     bc,
                         dams_user               t4
                   WHERE t1.procinst_id = t3.procinst_id
                     AND t3.creator_stru = bc.stru_id
                     AND bc.dept_stru_id = v_dept_stru_id
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                        --AND t3.creator_id = t4.ssic_id
                     AND EXISTS (SELECT '1'
                            FROM viw_dams_wf_all_task t2
                           WHERE t2.procinst_id = t3.procinst_id
                             AND t2.ssic_id = t4.ssic_id)
                     AND t4.name = i_applyer
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id)
                  UNION
                  SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task     t1,
                         viw_dams_wf_all_process  t3,
                         dams_user                t4,
                         sx_pay_app_busi_trip_det sx
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND sx.business_id = t1.business_id
                     AND sx.ssic_id = t4.ssic_id
                     AND t4.name = i_applyer
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id));

          OPEN o_list_cur FOR
            SELECT task_id,
                   task_name,
                   ssic_id,
                   decode(close_flag,
                          1,
                          '',
                          func_get_cur_dealer(business_id)) user_name,
                   stru_id,
                   prev_task_id,
                   prev_ssic_id,
                   prev_person_name,
                   prev_stru_id,
                   procinst_id,
                   process_id,
                   process_name,
                   form_id,
                   pckg_dams_util.func_decode('BIZ_TYPE',
                                              business_type,
                                              'DAMS') business_type,
                   business_id,
                   title,
                   create_time,
                   take_time,
                   end_time,
                   task_state,
                   outgoing_name,
                   opinion,
                   applyer,
                   apply_time,
                   complete_time,
                   close_flag,
                   '' url
              FROM (SELECT task_id,
                           task_name,
                           ssic_id,
                           user_name,
                           stru_id,
                           prev_task_id,
                           prev_ssic_id,
                           prev_person_name,
                           prev_stru_id,
                           procinst_id,
                           process_id,
                           process_name,
                           form_id,
                           pckg_dams_util.func_decode('BIZ_TYPE',
                                                      business_type,
                                                      'DAMS') business_type,
                           business_id,
                           title,
                           create_time,
                           take_time,
                           end_time,
                           task_state,
                           outgoing_name,
                           opinion,
                           applyer,
                           apply_time,
                           complete_time,
                           close_flag,
                           row_number() over(ORDER BY create_time DESC) rn
                      FROM (SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.ssic_id) user_name,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t3.creator_id) apply_name,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task    t1,
                                   viw_dams_wf_all_process t3,
                                   dams_branch_catalog     bc,
                                   dams_user               t4
                             WHERE t1.procinst_id = t3.procinst_id
                               AND t3.creator_stru = bc.stru_id
                               AND bc.dept_stru_id = v_dept_stru_id
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                                  --AND t3.creator_id = t4.ssic_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_task t2
                                     WHERE t2.procinst_id = t3.procinst_id
                                       AND t2.ssic_id = t4.ssic_id)
                               AND t4.name = i_applyer
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)
                            UNION
                            SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.ssic_id) user_name,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t3.creator_id) apply_name,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task     t1,
                                   viw_dams_wf_all_process  t3,
                                   dams_user                t4,
                                   sx_pay_app_busi_trip_det sx
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t4
                                     WHERE t4.pdid = t3.process_id
                                       AND t4.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                               AND sx.business_id = t1.business_id
                               AND sx.ssic_id = t4.ssic_id
                               AND t4.name = i_applyer
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)))
             WHERE rn >= to_number(i_beg_num)
               AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
        ELSE
          --一般人员
          SELECT COUNT(1)
            INTO o_total_num
            FROM (SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task    t1,
                         viw_dams_wf_all_process t3,
                         dams_user               t4
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_task t2
                           WHERE t2.procinst_id = t3.procinst_id
                             AND t2.ssic_id = i_ssic_id)
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                        --AND t3.creator_id = t4.ssic_id
                     AND EXISTS (SELECT '1'
                            FROM viw_dams_wf_all_task t2
                           WHERE t2.procinst_id = t3.procinst_id
                             AND t2.ssic_id = t4.ssic_id)
                     AND t4.name = i_applyer
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id)
                  UNION
                  SELECT t3.procinst_id
                    FROM viw_dams_wf_all_task     t1,
                         viw_dams_wf_all_process  t3,
                         dams_user                t4,
                         sx_pay_app_busi_trip_det sx
                   WHERE t1.procinst_id = t3.procinst_id
                     AND EXISTS
                   (SELECT '1'
                            FROM dams_wf_process_conf t4
                           WHERE t4.pdid = t3.process_id
                             AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
                     AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
                     AND t3.business_type =
                         nvl(i_biz_type, t3.business_type)
                     AND EXISTS (SELECT '1'
                            FROM viw_dams_wf_all_task t2
                           WHERE t2.procinst_id = t3.procinst_id
                             AND t2.ssic_id = i_ssic_id)
                     AND sx.business_id = t1.business_id
                     AND sx.ssic_id = t4.ssic_id
                     AND t4.name = i_applyer
                     AND EXISTS
                   (SELECT '1'
                            FROM viw_dams_wf_all_process t5
                           WHERE t5.procinst_id = t3.procinst_id
                             AND t5.start_time >=
                                 to_date(nvl(i_begin_date, v_min_date),
                                         'YYYY-MM-DD')
                             AND t5.start_time <= v_end_date)
                     AND t1.create_time =
                         (SELECT MAX(tt1.create_time)
                            FROM viw_dams_wf_all_task tt1
                           WHERE tt1.procinst_id = t3.procinst_id));

          OPEN o_list_cur FOR
            SELECT task_id,
                   task_name,
                   ssic_id,
                   decode(close_flag,
                          1,
                          '',
                          func_get_cur_dealer(business_id)) user_name,
                   stru_id,
                   prev_task_id,
                   prev_ssic_id,
                   prev_person_name,
                   prev_stru_id,
                   procinst_id,
                   process_id,
                   process_name,
                   form_id,
                   pckg_dams_util.func_decode('BIZ_TYPE',
                                              business_type,
                                              'DAMS') business_type,
                   business_id,
                   title,
                   create_time,
                   take_time,
                   end_time,
                   task_state,
                   outgoing_name,
                   opinion,
                   applyer,
                   apply_time,
                   complete_time,
                   close_flag,
                   '' url
              FROM (SELECT task_id,
                           task_name,
                           ssic_id,
                           user_name,
                           stru_id,
                           prev_task_id,
                           prev_ssic_id,
                           prev_person_name,
                           prev_stru_id,
                           procinst_id,
                           process_id,
                           process_name,
                           form_id,
                           pckg_dams_util.func_decode('BIZ_TYPE',
                                                      business_type,
                                                      'DAMS') business_type,
                           business_id,
                           title,
                           create_time,
                           take_time,
                           end_time,
                           task_state,
                           outgoing_name,
                           opinion,
                           applyer,
                           apply_time,
                           complete_time,
                           close_flag,
                           row_number() over(ORDER BY create_time DESC) rn
                      FROM (SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.ssic_id) user_name,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t3.creator_id) apply_name,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task    t1,
                                   viw_dams_wf_all_process t3,
                                   dams_user               t4
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t9
                                     WHERE t9.pdid = t3.process_id
                                       AND t9.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_task t2
                                     WHERE t2.procinst_id = t3.procinst_id
                                       AND t2.ssic_id = i_ssic_id)
                               AND t1.close_flag =
                                   nvl(i_close_flag, t1.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                                  --AND t3.creator_id = t4.ssic_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_task t2
                                     WHERE t2.procinst_id = t3.procinst_id
                                       AND t2.ssic_id = t4.ssic_id)
                               AND t4.name = i_applyer
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)
                            UNION
                            SELECT t1.task_id,
                                   t1.task_name,
                                   t1.ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.ssic_id) user_name,
                                   t1.stru_id,
                                   t1.role_id,
                                   t1.prev_task_id,
                                   t1.prev_ssic_id,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                   t1.prev_stru_id,
                                   t1.prev_role_id,
                                   t3.procinst_id,
                                   t3.process_id,
                                   t3.process_name,
                                   t1.form_id,
                                   t3.business_type,
                                   t3.business_id,
                                   t3.title,
                                   t1.create_time,
                                   t1.take_time,
                                   t1.end_time,
                                   t1.task_state,
                                   t1.outgoing_name,
                                   t1.opinion,
                                   t3.creator_id applyer,
                                   (SELECT u.name
                                      FROM dams_user u
                                     WHERE u.ssic_id = t3.creator_id) apply_name,
                                   to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                                   t3.complete_time,
                                   t3.close_flag
                              FROM viw_dams_wf_all_task     t1,
                                   viw_dams_wf_all_process  t3,
                                   dams_user                t4,
                                   sx_pay_app_busi_trip_det sx
                             WHERE t1.procinst_id = t3.procinst_id
                               AND EXISTS
                             (SELECT '1'
                                      FROM dams_wf_process_conf t4
                                     WHERE t4.pdid = t3.process_id
                                       AND t4.sys_code =
                                           nvl(i_sys_code, t3.sys_code))
                               AND t3.close_flag =
                                   nvl(i_close_flag, t3.close_flag)
                               AND t3.business_type =
                                   nvl(i_biz_type, t3.business_type)
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_task t2
                                     WHERE t2.procinst_id = t3.procinst_id
                                       AND t2.ssic_id = i_ssic_id)
                               AND sx.business_id = t1.business_id
                               AND sx.ssic_id = t4.ssic_id
                               AND t4.name = i_applyer
                               AND EXISTS
                             (SELECT '1'
                                      FROM viw_dams_wf_all_process t5
                                     WHERE t5.procinst_id = t3.procinst_id
                                       AND t5.start_time >=
                                           to_date(nvl(i_begin_date,
                                                       v_min_date),
                                                   'YYYY-MM-DD')
                                       AND t5.start_time <= v_end_date)
                               AND t1.create_time =
                                   (SELECT MAX(tt1.create_time)
                                      FROM viw_dams_wf_all_task tt1
                                     WHERE tt1.procinst_id = t3.procinst_id)))
             WHERE rn >= to_number(i_beg_num)
               AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
        END IF;
      END IF;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_sx_task_qry;
  
  PROCEDURE proc_ab_task_qry(i_ssic_id    IN VARCHAR2, --统一认证号
                             i_biz_type   IN VARCHAR2, --业务类型
                             i_sys_code   IN VARCHAR2, --系统编码
                             i_close_flag IN VARCHAR2, --流程结束标志
                             i_applyer    IN VARCHAR2, --申请人
                             i_app_stru   IN VARCHAR2, --申请部门
                             i_begin_date IN VARCHAR2, --查询开始时间
                             i_end_date   IN VARCHAR2, --查询结束时间
                             i_view_type  IN VARCHAR2, --1，查询全部，2查询有权限的数据
                             i_beg_num    IN VARCHAR2, --查询开始记录
                             i_fetch_num  IN VARCHAR2, --查询记录数
                             o_retcode    OUT VARCHAR2, --返回值
                             o_total_num  OUT VARCHAR2, --记录总数
                             o_list_cur   OUT SYS_REFCURSOR --查询列表游标
                             ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                      i_sys_code || '|' || i_close_flag || '|' ||
                                      i_applyer || '|' || i_app_stru || '|' ||
                                      i_begin_date || '|' || i_end_date || '|' ||
                                      i_beg_num || '|' || i_fetch_num;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_AB_TASK_QRY';
    v_step        db_log.step_no%TYPE := 0;
    v_min_date    VARCHAR2(10) := '1900-01-01';
    v_end_date    DATE := SYSDATE;
    v_num         NUMBER;
    v_status_code VARCHAR2(2) := '1';
    v_count       NUMBER;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    IF i_end_date IS NULL OR i_end_date = '' THEN
      NULL;
    ELSE
      v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
    END IF;
    SELECT COUNT(1)
      INTO v_count
      FROM dams_user_role_rel ur
     WHERE ur.ssic_id = i_ssic_id
       AND ur.role_id = 'AB007'
       AND ur.auth_state IN ('1', '2');
    IF v_count > 0 THEN
      --有通知发起人角色
      v_status_code := '2';
    ELSE
      --没有通知发起人角色
      v_status_code := '1';
    END IF;
    IF v_status_code = '1' THEN
      --没有通知发起人角色
      SELECT COUNT(1)
        INTO o_total_num
        FROM viw_dams_wf_all_task    t1,
             viw_dams_wf_all_process t3,
             dams_user               t4
       WHERE t1.procinst_id = t3.procinst_id
            --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
         AND EXISTS (SELECT '1'
                FROM dams_wf_process_conf t4
               WHERE t4.pdid = t3.process_id
                 AND t4.sys_code = nvl(i_sys_code, t3.sys_code))
         AND EXISTS
       (SELECT '1'
                FROM viw_dams_wf_all_task t2
               WHERE t2.procinst_id = t3.procinst_id
                 AND t2.ssic_id = i_ssic_id
                 AND t2.task_state IN ('completed'))
         AND t3.close_flag = nvl(i_close_flag, t3.close_flag)
         AND t3.business_type = nvl(i_biz_type, t3.business_type)
         AND t3.creator_id = t4.ssic_id
            --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
         AND t4.name = nvl(i_applyer, t4.name)
         AND EXISTS
       (SELECT '1'
                FROM viw_dams_wf_all_process t5
               WHERE t5.procinst_id = t3.procinst_id
                 AND t5.start_time >=
                     to_date(nvl(i_begin_date, v_min_date), 'YYYY-MM-DD')
                 AND t5.start_time <= v_end_date)
         AND t1.create_time =
             (SELECT MAX(tt1.create_time)
                FROM viw_dams_wf_all_task tt1
               WHERE tt1.procinst_id = t3.procinst_id);
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT t1.task_id,
                       t1.task_name,
                       t1.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.ssic_id) user_name,
                       t1.stru_id,
                       t1.role_id,
                       t1.prev_task_id,
                       t1.prev_ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                       t1.prev_stru_id,
                       t1.prev_role_id,
                       t3.procinst_id,
                       t3.process_id,
                       t3.process_name,
                       t1.form_id,
                       t3.business_type,
                       t3.business_id,
                       t3.title,
                       t1.create_time,
                       t1.take_time,
                       t1.end_time,
                       t1.task_state,
                       t1.outgoing_name,
                       t1.opinion,
                       t3.creator_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t3.creator_id) apply_name,
                       to_char(t3.start_time, 'YYYY-MM-DD') apply_time,
                       t3.complete_time,
                       t3.close_flag,
                       row_number() over(ORDER BY t1.create_time DESC) rn
                  FROM viw_dams_wf_all_task    t1,
                       viw_dams_wf_all_process t3,
                       dams_user               t4
                 WHERE t1.procinst_id = t3.procinst_id
                      --AND T1.SYS_CODE = NVL(I_SYS_CODE, T1.SYS_CODE)
                   AND EXISTS
                 (SELECT '1'
                          FROM dams_wf_process_conf t9
                         WHERE t9.pdid = t3.process_id
                           AND t9.sys_code = nvl(i_sys_code, t3.sys_code))
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_task t2
                         WHERE t2.procinst_id = t3.procinst_id
                           AND t2.ssic_id = i_ssic_id
                           AND t2.task_state IN ('completed'))
                   AND t1.close_flag = nvl(i_close_flag, t1.close_flag)
                   AND t3.business_type = nvl(i_biz_type, t3.business_type)
                   AND t3.creator_id = t4.ssic_id
                      --AND T3.CREATOR_STRU = NVL(I_APP_STRU,T3.CREATOR_STRU)
                   AND t4.name = nvl(i_applyer, t4.name)
                   AND EXISTS
                 (SELECT '1'
                          FROM viw_dams_wf_all_process t5
                         WHERE t5.procinst_id = t3.procinst_id
                           AND t5.start_time >=
                               to_date(nvl(i_begin_date, v_min_date),
                                       'YYYY-MM-DD')
                           AND t5.start_time <= v_end_date)
                   AND t1.create_time =
                       (SELECT MAX(tt1.create_time)
                          FROM viw_dams_wf_all_task tt1
                         WHERE tt1.procinst_id = t3.procinst_id)
                 ORDER BY t1.create_time DESC)
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    ELSIF v_status_code = '2' THEN
      --有通知发起人角色
      SELECT COUNT(1)
        INTO o_total_num
        FROM viw_dams_wf_all_task t1,
             viw_dams_wf_all_process t2,
             dams_noti_app_feedback t3,
             (SELECT c.major_stru_id AS stru_id
                FROM dams_branch_catalog c, dams_user_role_rel r
               WHERE c.stru_id = r.stru_id
                 AND r.ssic_id = i_ssic_id
                 AND r.role_id = 'AB007'
                 AND r.auth_state IN ('1', '2')
              UNION
              SELECT c.dept_stru_id AS stru_id
                FROM dams_branch_catalog c, dams_user_role_rel r
               WHERE c.stru_id = r.stru_id
                 AND r.ssic_id = i_ssic_id
                 AND r.role_id = 'AB007'
                 AND r.auth_state IN ('1', '2')
              UNION
              SELECT ur.stru_id AS stru_id
                FROM dams_user_role_rel ur
               WHERE ur.ssic_id = i_ssic_id
                 AND ur.role_id = 'AB007'
                 AND ur.auth_state IN ('1', '2')) t4
       WHERE t1.procinst_id = t2.procinst_id
         AND t1.business_id = t3.business_id
         AND t3.stru_id = t4.stru_id;
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               ssic_id,
               decode(close_flag, 1, '', func_get_cur_dealer(business_id)) user_name,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               applyer,
               apply_time,
               complete_time,
               close_flag
          FROM (SELECT t1.task_id,
                       t1.task_name,
                       t1.ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.ssic_id) user_name,
                       t1.stru_id,
                       t1.role_id,
                       t1.prev_task_id,
                       t1.prev_ssic_id,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                       t1.prev_stru_id,
                       t1.prev_role_id,
                       t2.procinst_id,
                       t2.process_id,
                       t2.process_name,
                       t1.form_id,
                       t2.business_type,
                       t2.business_id,
                       t2.title,
                       t1.create_time,
                       t1.take_time,
                       t1.end_time,
                       t1.task_state,
                       t1.outgoing_name,
                       t1.opinion,
                       t2.creator_id applyer,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = t2.creator_id) apply_name,
                       to_char(t2.start_time, 'YYYY-MM-DD') apply_time,
                       t2.complete_time,
                       t2.close_flag,
                       row_number() over(ORDER BY t1.create_time DESC) rn
                  FROM viw_dams_wf_all_task t1,
                       viw_dams_wf_all_process t2,
                       dams_noti_app_feedback t3,
                       (SELECT c.major_stru_id AS stru_id
                          FROM dams_branch_catalog c, dams_user_role_rel r
                         WHERE c.stru_id = r.stru_id
                           AND r.ssic_id = i_ssic_id
                           AND r.role_id = 'AB007'
                           AND r.auth_state IN ('1', '2')
                        UNION
                        SELECT c.dept_stru_id AS stru_id
                          FROM dams_branch_catalog c, dams_user_role_rel r
                         WHERE c.stru_id = r.stru_id
                           AND r.ssic_id = i_ssic_id
                           AND r.role_id = 'AB007'
                           AND r.auth_state IN ('1', '2')
                        UNION
                        SELECT ur.stru_id AS stru_id
                          FROM dams_user_role_rel ur
                         WHERE ur.ssic_id = i_ssic_id
                           AND ur.role_id = 'AB007'
                           AND ur.auth_state IN ('1', '2')) t4
                 WHERE t1.procinst_id = t2.procinst_id
                   AND t1.business_id = t3.business_id
                   AND t3.stru_id = t4.stru_id)
         WHERE rn >= to_number(i_beg_num)
           AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_ab_task_qry;
  
  PROCEDURE proc_get_latest_draft_task(i_ssic_id IN VARCHAR2, --统一认证号
                                       o_retcode OUT VARCHAR2, --返回值
                                       o_msg     OUT VARCHAR2,
                                       o_detail  OUT SYS_REFCURSOR --查询列表游标
                                       ) IS
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_GET_LATEST_DRAFT_TASK';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    OPEN o_detail FOR
      SELECT task_id,
             task_name,
             user_name,
             stru_id,
             role_id,
             prev_task_id,
             prev_task_name,
             prev_ssic_id,
             prev_person_name,
             prev_stru_id,
             procinst_id,
             process_id,
             process_name,
             form_id,
             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
             business_id,
             title,
             create_time,
             take_time,
             end_time,
             task_state,
             outgoing_name,
             opinion,
             apply_name,
             apply_time,
             app_stru,
             app_stur_name
        FROM (SELECT 0 task_id,
                     '拟稿' task_name,
                     t.applyer,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t.applyer) user_name,
                     t.app_stru stru_id,
                     '' role_id,
                     0 prev_task_id,
                     '拟稿' prev_task_name,
                     t.applyer prev_ssic_id,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t.applyer) prev_person_name,
                     '' prev_stru_id,
                     '' prev_role_id,
                     t.app_id procinst_id,
                     '' process_id,
                     '' process_name,
                     func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
                     t.biz_type business_type,
                     t.app_id business_id,
                     t.info title,
                     to_char(t.app_time, 'YYYY-MM-DD') create_time,
                     to_char(t.app_time, 'YYYY-MM-DD') take_time,
                     '' end_time,
                     'draft' task_state,
                     '' outgoing_name,
                     '' opinion,
                     t.applyer applyer,
                     (SELECT u.name
                        FROM dams_user u
                       WHERE u.ssic_id = t.applyer) apply_name,
                     to_char(t.app_time, 'YYYY-MM-DD') apply_time,
                     t.app_stru,
                     (SELECT b.stru_sname
                        FROM dams_branch b
                       WHERE b.stru_id = t.app_stru) app_stur_name,
                     row_number() over(ORDER BY t.app_time DESC) rn
                FROM dams_save_info t,
                     (SELECT t2.pdid, MAX(t2.sys_code) sys_code
                        FROM dams_wf_process_conf t2
                       GROUP BY t2.pdid) t1
               WHERE t.applyer = i_ssic_id
                 AND t1.pdid = t.pdid
                 AND t.valid_flag = 1)
       WHERE rn = 1;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      o_msg := '异常！' || v_param || SQLERRM;
      pack_log.error(v_proc_name, pack_log.end_step, o_msg);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_get_latest_draft_task;
  
  PROCEDURE proc_inner_task_finish(i_app_id      IN VARCHAR2,
                                   i_business_id IN VARCHAR2,
                                   i_ssic_id     IN VARCHAR2, --可为空
                                   i_sys_code    IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2 --返回值
                                   ) IS
    v_param     db_log.info%TYPE := i_app_id || '|' || i_business_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_task.proc_inner_task_finish';
    v_step      db_log.step_no%TYPE := 0;
    v_count NUMBER(7);
  BEGIN
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := '0';
    proc_inner_task_finish(i_app_id,
                           i_business_id,
                           i_ssic_id,
                           i_sys_code,
                           --'',
                           o_retcode);
    pack_log.info(v_proc_name,  pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
  END proc_inner_task_finish;
  
  --PROCEDURE proc_inner_task_finish(i_app_id      IN VARCHAR2,
  --                                 i_business_id IN VARCHAR2,
  --                                 i_ssic_id     IN VARCHAR2, --可为空
  --                                 i_sys_code    IN VARCHAR2,
  --                                 o_retcode     OUT VARCHAR2 --返回值
  --                                 ) IS
  --  v_param     db_log.info%TYPE := i_app_id || '|' || i_business_id;
  --  v_proc_name db_log.proc_name%TYPE := 'pckg_dams_task.proc_inner_task_finish';
  --  v_step      db_log.step_no%TYPE := 0;
  --
  --  v_count NUMBER(7);
  --BEGIN
  --  pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  --  o_retcode := '0';
  --  proc_inner_task_finish(i_app_id,
  --                         i_business_id,
  --                         i_ssic_id,
  --                         i_sys_code,
  --                         --'',
  --                         o_retcode);
  --  pack_log.info(v_proc_name,  pack_log.end_step, pack_log.end_msg || v_param);
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --    o_retcode := '-1';
  --    ROLLBACK;
  --END proc_inner_task_finish;
  
  PROCEDURE proc_qry_inner_todo_url(i_ssic_id     IN VARCHAR2, --统一认证号
                                    i_task_id     IN VARCHAR2, --业务类型
                                    i_business_id IN VARCHAR2, --系统编码
                                    i_sys_code    IN VARCHAR2,
                                    o_ret_code    OUT VARCHAR2, --返回值
                                    o_msg         OUT VARCHAR2,
                                    o_url         OUT VARCHAR2 --记录总数
                                    ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_task_id || '|' ||
                                    i_business_id || '|' || i_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.proc_qry_inner_todo_url';

    v_count NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := '0';
    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT t1.url
              FROM dams_inner_todo_task t1
             WHERE t1.ssic_id = i_ssic_id
               AND t1.task_id = i_task_id
               AND t1.business_id = i_business_id
            UNION
            SELECT t2.url
              FROM dams_inner_over_task t2
             WHERE t2.ssic_id = i_ssic_id
               AND t2.task_id = i_task_id
               AND t2.business_id = i_business_id);
    IF v_count = 0 THEN
      o_ret_code := '1';
      o_msg      := '无此记录，可能已被废止或您无权限访问';
    END IF;

    SELECT MAX(url)
      INTO o_url
      FROM (SELECT t1.url
              FROM dams_inner_todo_task t1
             WHERE t1.ssic_id = i_ssic_id
               AND t1.task_id = i_task_id
               AND t1.business_id = i_business_id
            UNION
            SELECT t2.url
              FROM dams_inner_over_task t2
             WHERE t2.ssic_id = i_ssic_id
               AND t2.task_id = i_task_id
               AND t2.business_id = i_business_id);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_qry_inner_todo_url;
  
  PROCEDURE proc_inner_task_finish_with_jz(i_app_id      IN VARCHAR2,
                                           i_business_id IN VARCHAR2,
                                           i_ssic_id     IN VARCHAR2, --可为空
                                           i_sys_code    IN VARCHAR2,
                                           i_url         IN VARCHAR2, --更新url
                                           o_retcode     OUT VARCHAR2 --返回值
                                           ) IS
    v_param     db_log.info%TYPE := i_app_id || '|' || i_business_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_task.proc_inner_task_finish_with_jz';
    v_step      db_log.step_no%TYPE := 0;

    v_count NUMBER(7);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    UPDATE dams_inner_todo_task t
       SET t.task_state = 'completed', t.end_time = SYSDATE, t.url = i_url
     WHERE t.task_id = i_app_id
       AND t.business_id = i_business_id
       AND t.sys_code = i_sys_code
       AND (t.ssic_id = i_ssic_id OR i_ssic_id IS NULL);

    UPDATE dams_inner_todo_task t
       SET t.task_state = 'completed'
     WHERE t.task_id = i_app_id
       AND t.business_id = i_business_id
       AND t.sys_code = i_sys_code;

    SELECT COUNT(1)
      INTO v_count
      FROM dams_inner_todo_task t
     WHERE t.business_id = i_business_id
       AND t.sys_code = i_sys_code
       AND t.task_state <> 'completed'
       AND rownum = 1;

    IF v_count = 0 THEN
      MERGE INTO dams_inner_over_task dt
      USING (SELECT t.task_id,
                    t.ssic_id,
                    t.prev_task_name,
                    t.prev_ssic_id,
                    t.create_time,
                    t.take_time,
                    t.end_time,
                    t.task_state,
                    t.business_id,
                    t.business_type,
                    t.title,
                    t.creator_id,
                    t.sys_code,
                    t.url,
                    t.in_sys_seq,
                    t.creator_stru
               FROM dams_inner_todo_task t
              WHERE t.business_id = i_business_id
                AND t.sys_code = i_sys_code) a
      ON (dt.task_id = a.task_id AND dt.business_id = a.business_id AND dt.sys_code = a.sys_code AND dt.ssic_id = a.ssic_id AND dt.take_time = nvl(a.take_time, a.end_time))
      WHEN NOT MATCHED THEN
        INSERT
          (task_id,
           ssic_id,
           prev_task_name,
           prev_ssic_id,
           create_time,
           take_time,
           end_time,
           task_state,
           business_id,
           business_type,
           title,
           creator_id,
           sys_code,
           url,
           in_sys_seq,
           creator_stru)
        VALUES
          (a.task_id,
           a.ssic_id,
           a.prev_task_name,
           a.prev_ssic_id,
           a.create_time,
           nvl(a.take_time, a.end_time),
           a.end_time,
           a.task_state,
           a.business_id,
           a.business_type,
           a.title,
           a.creator_id,
           a.sys_code,
           a.url,
           a.in_sys_seq,
           a.creator_stru);
      DELETE FROM dams_inner_todo_task t  WHERE t.business_id = i_business_id AND t.sys_code = i_sys_code;
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
  END proc_inner_task_finish_with_jz;
  
  PROCEDURE proc_qry_todo_task_num(i_ssic_id     IN VARCHAR2, --统一认证号
                                   i_biz_type    IN VARCHAR2, --业务类型
                                   i_sys_code    IN VARCHAR2, --系统编码
                                   i_business_id IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2, --返回值
                                   o_total_num   OUT VARCHAR2 --记录总数
                                   ) IS
    v_param       db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                      i_sys_code || '|' || i_business_id;
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.proc_qry_todo_task_num';
    v_step        db_log.step_no%TYPE := 0;
    v_sav_num     NUMBER;
    v_out_num     NUMBER;
    v_inner_num   NUMBER; --非流程待办数量
    v_person_name dams_user.name%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_retcode := '0';
    SELECT MAX(t.name)
      INTO v_person_name
      FROM dams_user t
     WHERE t.ssic_id = i_ssic_id;

    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_wf_task t1, dams_wf_process t4
     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
       AND EXISTS (SELECT '1'
              FROM jbpm4_participation t2
             WHERE t2.userid_ = i_ssic_id
               AND t2.task_ = t1.task_id)
       AND t1.procinst_id = t4.procinst_id
          --AND T1.IS_FIRST = 1
       AND t1.task_state IN ('open', 'suspended')
       AND nvl(t4.business_type, 0) =
           nvl(i_biz_type, nvl(t4.business_type, 0))
       AND t4.valid_flag = '1'
       AND (t1.business_id <> i_business_id or i_business_id is null);
    --草稿数量
    SELECT COUNT(1)
      INTO v_sav_num
      FROM dams_save_info t,
           (SELECT t2.pdid, MAX(t2.sys_code) sys_code
              FROM dams_wf_process_conf t2
             GROUP BY t2.pdid) t1
     WHERE t.applyer = i_ssic_id
       AND t1.pdid = t.pdid
       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
       AND nvl(t.biz_type, 0) = nvl(i_biz_type, nvl(t.biz_type, 0))
       AND t.valid_flag = 1
       AND (t.app_id <> i_business_id or i_business_id is null);
    --非流程待办
    SELECT COUNT(1)
      INTO v_inner_num
      FROM dams_inner_todo_task t
     WHERE t.ssic_id = i_ssic_id
       AND t.sys_code = nvl(i_sys_code, t.sys_code)
       AND nvl(t.business_type, 0) =
           nvl(i_biz_type, nvl(t.business_type, 0))
       AND (t.business_id <> i_business_id or i_business_id is null);
    --外部待办
    SELECT COUNT(1)
      INTO v_out_num
      FROM dams_outer_todo_task t
     WHERE t.ssic_id = i_ssic_id
       AND t.sys_code = nvl(i_sys_code, t.sys_code)
       AND t.task_state = 'open'
       AND nvl(t.business_type, 0) =
           nvl(i_biz_type, nvl(t.business_type, 0))
       AND (t.business_id <> i_business_id or i_business_id is null);
    o_total_num := o_total_num + v_inner_num + v_sav_num + v_out_num;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      RETURN;
  END proc_qry_todo_task_num;
  
  --PROCEDURE proc_todo_task_ac_qry(i_ssic_id   IN VARCHAR2, --统一认证号
  --                                i_biz_type  IN VARCHAR2, --业务类型
  --                                i_sys_code  IN VARCHAR2, --系统编码
  --                                i_status    IN VARCHAR2, --状态 空全部 1超期
  --                                i_beg_num   IN VARCHAR2, --查询开始记录
  --                                i_fetch_num IN VARCHAR2, --查询记录数
  --                                o_retcode   OUT VARCHAR2, --返回值
  --                                o_total_num OUT VARCHAR2, --记录总数
  --                                o_list_cur  OUT SYS_REFCURSOR --查询列表游标
  --                                ) IS
  --  v_param       db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
  --                                    i_sys_code || '|' || i_beg_num || '|' ||
  --                                    i_fetch_num;
  --  v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_TODO_TASK_QRY';
  --  v_step        db_log.step_no%TYPE := 0;
  --  v_sav_num     NUMBER;
  --  v_inner_num   NUMBER; --非流程待办数量
  --  v_chaoqi_num  NUMBER;
  --  v_person_name dams_user.name%TYPE;
  --  v_is_zjl VARCHAR2(1); --20150821 ggc 总经理标题去掉’草稿‘
  --BEGIN
  --  o_retcode := '0';
  --  SELECT COUNT(1)
  --    INTO v_is_zjl
  --    FROM dams_user_role_rel t
  --   WHERE t.ssic_id = i_ssic_id
  --     AND t.sys_code = 'GW'
  --     AND t.role_id = 'DR00000004'
  --     AND t.auth_state IN ('0', '1', '2');
  --  SELECT MAX(t.name)
  --    INTO v_person_name
  --    FROM dams_user t
  --   WHERE t.ssic_id = i_ssic_id;
  --  IF i_status IS NULL THEN
  --    SELECT COUNT(1)
  --      INTO o_total_num
  --      FROM dams_wf_task t1, dams_wf_process t4
  --     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
  --       AND EXISTS (SELECT '1'
  --              FROM jbpm4_participation t2
  --             WHERE t2.userid_ = i_ssic_id
  --               AND t2.task_ = t1.task_id)
  --       AND t1.procinst_id = t4.procinst_id
  --          --AND T1.IS_FIRST = 1
  --       AND t1.task_state IN ('open', 'suspended')
  --       AND nvl(t4.business_type, 0) =
  --           nvl(i_biz_type, nvl(t4.business_type, 0))
  --       AND t4.valid_flag = '1';
  --    --草稿数量
  --    SELECT COUNT(1)
  --      INTO v_sav_num
  --      FROM dams_save_info t,
  --           (SELECT t2.pdid, MAX(t2.sys_code) sys_code
  --              FROM dams_wf_process_conf t2
  --             GROUP BY t2.pdid) t1
  --     WHERE t.applyer = i_ssic_id
  --       AND t1.pdid = t.pdid
  --       AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
  --       AND nvl(t.biz_type, 0) = nvl(i_biz_type, nvl(t.biz_type, 0))
  --       AND t.valid_flag = 1;
  --    --非流程待办
  --    SELECT COUNT(1)
  --      INTO v_inner_num
  --      FROM dams_inner_todo_task t
  --     WHERE t.ssic_id = i_ssic_id
  --       AND t.sys_code = nvl(i_sys_code, t.sys_code)
  --       AND nvl(t.business_type, 0) =
  --           nvl(i_biz_type, nvl(t.business_type, 0))
  --       AND t.task_state = 'open';
  --    o_total_num := o_total_num + v_inner_num + v_sav_num;
  --    OPEN o_list_cur FOR
  --      SELECT task_id,
  --             task_name,
  --             user_name,
  --             stru_id,
  --             role_id,
  --             prev_task_id,
  --             prev_task_name,
  --             prev_ssic_id,
  --             prev_person_name,
  --             prev_stru_id,
  --             procinst_id,
  --             process_id,
  --             process_name,
  --             form_id,
  --             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
  --             business_id,
  --             title,
  --             create_time,
  --             take_time,
  --             end_time,
  --             task_state,
  --             outgoing_name,
  --             opinion,
  --             apply_name,
  --             apply_time,
  --             app_stru,
  --             app_stur_name,
  --             cfg_id,
  --             url, --kfzx-guoq03 add
  --             show_business_id,
  --             rn,
  --             star_level
  --        FROM (SELECT task_id,
  --                     task_name,
  --                     user_name,
  --                     stru_id,
  --                     role_id,
  --                     prev_task_id,
  --                     prev_task_name,
  --                     prev_ssic_id,
  --                     prev_person_name,
  --                     prev_stru_id,
  --                     procinst_id,
  --                     process_id,
  --                     process_name,
  --                     form_id,
  --                     business_type,
  --                     business_id,
  --                     title,
  --                     create_time,
  --                     take_time,
  --                     end_time,
  --                     task_state,
  --                     outgoing_name,
  --                     opinion,
  --                     apply_name,
  --                     apply_time,
  --                     app_stru,
  --                     app_stur_name,
  --                     cfg_id,
  --                     url,
  --                     coalesce(out_sys_seq, business_id) show_business_id,
  --                     row_number() over(ORDER BY tn.ordertime DESC) rn,
  --                     star_level
  --                FROM (SELECT '0' task_id,
  --                             '拟稿' task_name,
  --                             t.applyer,
  --                             v_person_name user_name,
  --                             t.app_stru stru_id,
  --                             '' role_id,
  --                             0 prev_task_id,
  --                             '拟稿' prev_task_name,
  --                             t.applyer prev_ssic_id,
  --                             v_person_name prev_person_name,
  --                             '' prev_stru_id,
  --                             '' prev_role_id,
  --                             t.app_id procinst_id,
  --                             '' process_id,
  --                             '' process_name,
  --                             func_get_form_id_from_pdid(t.pdid, t.app_id) form_id,
  --                             t.biz_type business_type,
  --                             t.app_id business_id,
  --                             t.info title,
  --                             to_char(t.app_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
  --                             to_char(t.app_time, 'YYYY-MM-DD') take_time,
  --                             '' end_time,
  --                             'draft' task_state,
  --                             '' outgoing_name,
  --                             '' opinion,
  --                             t.applyer applyer,
  --                             v_person_name apply_name,
  --                             to_char(t.app_time, 'YYYY-MM-DD') apply_time,
  --                             t.app_stru,
  --                             (SELECT b.stru_sname
  --                                FROM dams_branch b
  --                               WHERE b.stru_id = t.app_stru) app_stur_name,
  --                             NULL cfg_id,
  --                             '' url,
  --                             to_char(t.app_time, 'yyyymmddhh24miss') ordertime,
  --                             '' out_sys_seq,
  --                             '0' star_level
  --                      --kfzx-guoq03 add ,row_number() over(ORDER BY t.app_time DESC) rn
  --                        FROM dams_save_info t,
  --                             (SELECT t2.pdid, MAX(t2.sys_code) sys_code
  --                                FROM dams_wf_process_conf t2
  --                               GROUP BY t2.pdid) t1
  --                       WHERE t.applyer = i_ssic_id
  --                         AND t1.pdid = t.pdid
  --                         AND t1.sys_code = nvl(i_sys_code, t1.sys_code)
  --                         AND nvl(t.biz_type, 0) =
  --                             nvl(i_biz_type, nvl(t.biz_type, 0))
  --                         AND t.valid_flag = 1
  --                      UNION ALL (SELECT to_char(t1.task_id),
  --                                       t1.task_name,
  --                                       t1.ssic_id,
  --                                       v_person_name user_name,
  --                                       t1.stru_id,
  --                                       t1.role_id,
  --                                       t1.prev_task_id,
  --                                       (SELECT wt.task_name
  --                                          FROM dams_wf_task wt
  --                                         WHERE wt.task_id = t1.prev_task_id) prev_task_name,
  --                                       t1.prev_ssic_id,
  --                                       (SELECT u.name
  --                                          FROM dams_user u
  --                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
  --                                       t1.prev_stru_id,
  --                                       t1.prev_role_id,
  --                                       t4.procinst_id,
  --                                       t4.process_id,
  --                                       t4.process_name,
  --                                       t1.form_id,
  --                                       t4.business_type,
  --                                       t4.business_id,
  --                                       t4.title,
  --                                       to_char(t1.create_time,
  --                                               'YYYY-MM-DD HH24:MI:SS') create_time,
  --                                       to_char(t1.take_time, 'YYYY-MM-DD') take_time,
  --                                       to_char(t1.end_time, 'YYYY-MM-DD') end_time,
  --                                       t1.task_state,
  --                                       t1.outgoing_name,
  --                                       t1.opinion,
  --                                       t4.creator_id applyer,
  --                                       (SELECT u.name
  --                                          FROM dams_user u
  --                                         WHERE u.ssic_id = t4.creator_id) apply_name,
  --                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
  --                                       t4.creator_stru app_stru,
  --                                       (SELECT b.stru_sname
  --                                          FROM dams_branch b
  --                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
  --                                       t4.cfg_id cfgid,
  --                                       '' url,
  --                                       to_char(t1.create_time,
  --                                               'yyyymmddhh24miss') ordertime,
  --                                       '' out_sys_seq,
  --                                       (SELECT s.star_level
  --                                          FROM ac_supervision_info s
  --                                         WHERE s.business_id =
  --                                               t1.business_id) star_level
  --                                --kfzx-guoq03 add , row_number() over(ORDER BY t1.create_time DESC) + v_sav_num rn
  --                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
  --                                 WHERE t4.sys_code =
  --                                       nvl(i_sys_code, t4.sys_code)
  --                                   AND EXISTS
  --                                 (SELECT '1'
  --                                          FROM jbpm4_participation t2
  --                                         WHERE t2.userid_ = i_ssic_id
  --                                           AND t2.task_ = t1.task_id)
  --                                   AND t1.procinst_id = t4.procinst_id
  --                                      --AND T1.IS_FIRST = 1
  --                                   AND t1.task_state IN
  --                                       ('open', 'suspended')
  --                                   AND nvl(t4.business_type, 0) =
  --                                       nvl(i_biz_type,
  --                                           nvl(t4.business_type, 0))
  --                                   AND t4.valid_flag = '1')
  --                      --非流程待办
  --                      UNION ALL (SELECT i.task_id,
  --                                       '' task_name,
  --                                       i.ssic_id,
  --                                       v_person_name user_name,
  --                                       '' stru_id,
  --                                       'INNER' role_id, --非流程待办
  --                                       NULL prev_task_id,
  --                                       i.prev_task_name prev_task_name,
  --                                       i.prev_ssic_id,
  --                                       (SELECT u.name
  --                                          FROM dams_user u
  --                                         WHERE u.ssic_id = i.prev_ssic_id) prev_person_name,
  --                                       '' prev_stru_id,
  --                                       '' prev_role_id,
  --                                       NULL procinst_id,
  --                                       NULL process_id,
  --                                       '' process_name,
  --                                       NULL form_id,
  --                                       i.business_type,
  --                                       i.business_id,
  --                                       i.title,
  --                                       to_char(i.create_time, 'YYYY-MM-DD HH24:MI:SS') create_time,
  --                                       to_char(i.take_time, 'yyyy-mm-dd') take_time,
  --                                       '' end_time,
  --                                       i.task_state,
  --                                       '' outgoing_name,
  --                                       '' opinion,
  --                                       i.creator_id applyer,
  --                                       (SELECT u.name
  --                                          FROM dams_user u
  --                                         WHERE u.ssic_id = i.creator_id) apply_name,
  --                                       '' apply_time,
  --                                       i.creator_stru app_stru,
  --                                       (SELECT b.stru_sname
  --                                          FROM dams_branch b
  --                                         WHERE b.stru_id = i.creator_stru) app_stru_name,
  --                                       NULL cfgid,
  --                                       i.url,
  --                                       to_char(i.create_time, 'yyyymmddhh24miss') ordertime,
  --                                       i.in_sys_seq,
  --                                       (SELECT s.star_level
  --                                          FROM ac_supervision_info s
  --                                         WHERE s.business_id = i.business_id) star_level
  --                                  FROM dams_inner_todo_task i
  --                                 WHERE i.ssic_id = i_ssic_id
  --                                   AND i.task_state = 'open'
  --                                   AND i.sys_code =
  --                                       nvl(i_sys_code, i.sys_code)
  --                                   AND nvl(i.business_type, 0) =
  --                                       nvl(i_biz_type,
  --                                           nvl(i.business_type, 0)))) tn)
  --       WHERE rn >= to_number(i_beg_num)
  --         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
  --  ELSE
  --    SELECT COUNT(1)
  --      INTO o_total_num
  --      FROM dams_wf_task t1, dams_wf_process t4, ac_supervision_info a
  --     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
  --       AND EXISTS
  --     (SELECT '1'
  --              FROM jbpm4_participation t2
  --             WHERE t2.userid_ = i_ssic_id
  --               AND t2.task_ = t1.task_id)
  --       AND t1.procinst_id = t4.procinst_id
  --          --AND T1.IS_FIRST = 1
  --       AND t1.task_state IN ('open', 'suspended')
  --       AND nvl(t4.business_type, 0) =
  --           nvl(i_biz_type, nvl(t4.business_type, 0))
  --       AND t4.valid_flag = '1'
  --       AND t4.business_id = a.business_id
  --       AND a.source_business_id IS NOT NULL
  --       AND pckg_ac_supervision.fun_cal_ot_days(pckg_ac_supervision.fun_branch_bank(a.drafter_unit_id), a.feedback_date) > 0;
  --    SELECT COUNT(1)
  --      INTO v_chaoqi_num
  --      FROM dams_wf_task            t1,
  --           dams_wf_process         t4,
  --           ac_supervision_info     a,
  --           ac_supervision_chb_unit b
  --     WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
  --       AND EXISTS (SELECT '1'
  --              FROM jbpm4_participation t2
  --             WHERE t2.userid_ = i_ssic_id
  --               AND t2.task_ = t1.task_id)
  --       AND t1.procinst_id = t4.procinst_id
  --          --AND T1.IS_FIRST = 1
  --       AND t1.task_state IN ('open', 'suspended')
  --       AND nvl(t4.business_type, 0) =
  --           nvl(i_biz_type, nvl(t4.business_type, 0))
  --       AND t4.valid_flag = '1'
  --       AND t4.business_id = a.business_id
  --       AND a.source_business_id IS NULL
  --       AND b.business_id = a.business_id
  --       AND b.type = '1'
  --       AND b.ot_days > 0;
  --    o_total_num := o_total_num + v_chaoqi_num;
  --    OPEN o_list_cur FOR
  --      SELECT task_id,
  --             task_name,
  --             user_name,
  --             stru_id,
  --             role_id,
  --             prev_task_id,
  --             prev_task_name,
  --             prev_ssic_id,
  --             prev_person_name,
  --             prev_stru_id,
  --             procinst_id,
  --             process_id,
  --             process_name,
  --             form_id,
  --             pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
  --             business_id,
  --             title,
  --             create_time,
  --             take_time,
  --             end_time,
  --             task_state,
  --             outgoing_name,
  --             opinion,
  --             apply_name,
  --             apply_time,
  --             app_stru,
  --             app_stur_name,
  --             cfg_id,
  --             url, --kfzx-guoq03 add
  --             show_business_id,
  --             rn,
  --             star_level
  --        FROM (SELECT task_id,
  --                     task_name,
  --                     user_name,
  --                     stru_id,
  --                     role_id,
  --                     prev_task_id,
  --                     prev_task_name,
  --                     prev_ssic_id,
  --                     prev_person_name,
  --                     prev_stru_id,
  --                     procinst_id,
  --                     process_id,
  --                     process_name,
  --                     form_id,
  --                     business_type,
  --                     business_id,
  --                     title,
  --                     create_time,
  --                     take_time,
  --                     end_time,
  --                     task_state,
  --                     outgoing_name,
  --                     opinion,
  --                     apply_name,
  --                     apply_time,
  --                     app_stru,
  --                     app_stur_name,
  --                     cfg_id,
  --                     url,
  --                     coalesce(out_sys_seq, business_id) show_business_id,
  --                     row_number() over(ORDER BY tn.ordertime DESC) rn,
  --                     star_level
  --                FROM ((SELECT to_char(t1.task_id) task_id,
  --                              t1.task_name,
  --                              t1.ssic_id,
  --                              v_person_name user_name,
  --                              t1.stru_id,
  --                              t1.role_id,
  --                              t1.prev_task_id,
  --                              (SELECT wt.task_name
  --                                 FROM dams_wf_task wt
  --                                WHERE wt.task_id = t1.prev_task_id) prev_task_name,
  --                              t1.prev_ssic_id,
  --                              (SELECT u.name
  --                                 FROM dams_user u
  --                                WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
  --                              t1.prev_stru_id,
  --                              t1.prev_role_id,
  --                              t4.procinst_id,
  --                              t4.process_id,
  --                              t4.process_name,
  --                              t1.form_id,
  --                              t4.business_type,
  --                              t4.business_id,
  --                              t4.title,
  --                              to_char(t1.create_time,
  --                                      'YYYY-MM-DD HH24:MI:SS') create_time,
  --                              to_char(t1.take_time, 'YYYY-MM-DD') take_time,
  --                              to_char(t1.end_time, 'YYYY-MM-DD') end_time,
  --                              t1.task_state,
  --                              t1.outgoing_name,
  --                              t1.opinion,
  --                              t4.creator_id applyer,
  --                              (SELECT u.name
  --                                 FROM dams_user u
  --                                WHERE u.ssic_id = t4.creator_id) apply_name,
  --                              to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
  --                              t4.creator_stru app_stru,
  --                              (SELECT b.stru_sname
  --                                 FROM dams_branch b
  --                                WHERE b.stru_id = t4.creator_stru) app_stur_name,
  --                              t4.cfg_id cfg_id,
  --                              '' url,
  --                              to_char(t1.create_time, 'yyyymmddhh24miss') ordertime,
  --                              '' out_sys_seq,
  --                              a.star_level
  --                         FROM dams_wf_task        t1,
  --                              dams_wf_process     t4,
  --                              ac_supervision_info a
  --                        WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
  --                          AND EXISTS
  --                        (SELECT '1'
  --                                 FROM jbpm4_participation t2
  --                                WHERE t2.userid_ = i_ssic_id
  --                                  AND t2.task_ = t1.task_id)
  --                          AND t1.procinst_id = t4.procinst_id
  --                             --AND T1.IS_FIRST = 1
  --                          AND t1.task_state IN ('open', 'suspended')
  --                          AND nvl(t4.business_type, 0) =
  --                              nvl(i_biz_type, nvl(t4.business_type, 0))
  --                          AND t4.valid_flag = '1'
  --                          AND t4.business_id = a.business_id
  --                          AND a.source_business_id IS NOT NULL
  --                          AND pckg_ac_supervision.fun_cal_ot_days(pckg_ac_supervision.fun_branch_bank(a.drafter_unit_id),
  --                                                                  a.feedback_date) > 0)
  --                     --督办部门查看超期
  --                      UNION ALL
  --                      (SELECT to_char(t1.task_id) task_id,
  --                              t1.task_name,
  --                              t1.ssic_id,
  --                              v_person_name user_name,
  --                              t1.stru_id,
  --                              t1.role_id,
  --                              t1.prev_task_id,
  --                              (SELECT wt.task_name
  --                                 FROM dams_wf_task wt
  --                                WHERE wt.task_id = t1.prev_task_id) prev_task_name,
  --                              t1.prev_ssic_id,
  --                              (SELECT u.name
  --                                 FROM dams_user u
  --                                WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
  --                              t1.prev_stru_id,
  --                              t1.prev_role_id,
  --                              t4.procinst_id,
  --                              t4.process_id,
  --                              t4.process_name,
  --                              t1.form_id,
  --                              t4.business_type,
  --                              t4.business_id,
  --                              t4.title,
  --                              to_char(t1.create_time,
  --                                      'YYYY-MM-DD HH24:MI:SS') create_time,
  --                              to_char(t1.take_time, 'YYYY-MM-DD') take_time,
  --                              to_char(t1.end_time, 'YYYY-MM-DD') end_time,
  --                              t1.task_state,
  --                              t1.outgoing_name,
  --                              t1.opinion,
  --                              t4.creator_id applyer,
  --                              (SELECT u.name
  --                                 FROM dams_user u
  --                                WHERE u.ssic_id = t4.creator_id) apply_name,
  --                              to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
  --                              t4.creator_stru app_stru,
  --                              (SELECT b.stru_sname
  --                                 FROM dams_branch b
  --                                WHERE b.stru_id = t4.creator_stru) app_stur_name,
  --                              t4.cfg_id cfg_id,
  --                              '' url,
  --                              to_char(t1.create_time, 'yyyymmddhh24miss') ordertime,
  --                              '' out_sys_seq,
  --                              a.star_level
  --                         FROM dams_wf_task            t1,
  --                              dams_wf_process         t4,
  --                              ac_supervision_info     a,
  --                              ac_supervision_chb_unit b
  --                        WHERE t4.sys_code = nvl(i_sys_code, t4.sys_code)
  --                          AND EXISTS
  --                        (SELECT '1'
  --                                 FROM jbpm4_participation t2
  --                                WHERE t2.userid_ = i_ssic_id
  --                                  AND t2.task_ = t1.task_id)
  --                          AND t1.procinst_id = t4.procinst_id
  --                             --AND T1.IS_FIRST = 1
  --                          AND t1.task_state IN ('open', 'suspended')
  --                          AND nvl(t4.business_type, 0) =
  --                              nvl(i_biz_type, nvl(t4.business_type, 0))
  --                          AND t4.valid_flag = '1'
  --                          AND t4.business_id = a.business_id
  --                          AND a.source_business_id IS NULL
  --                          AND b.business_id = a.business_id
  --                          AND b.type = '1'
  --                          AND b.ot_days > 0)) tn)
  --       WHERE rn >= to_number(i_beg_num)
  --         AND rn < to_number(i_beg_num) + to_number(i_fetch_num);
  --  END IF;
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    o_retcode := '-1';
  --    pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --END proc_todo_task_ac_qry;
  
  PROCEDURE proc_done_task_ac_qry(i_ssic_id   IN VARCHAR2, --统一认证号
                                  i_biz_type  IN VARCHAR2, --业务类型
                                  i_sys_code  IN VARCHAR2, --系统编码
                                  i_status    IN VARCHAR2, --状态 1-承办部门未接收 2-承办部门未完成 3-承办部门已超期
                                  i_beg_num   IN VARCHAR2, --查询开始记录
                                  i_fetch_num IN VARCHAR2, --查询记录数
                                  o_retcode   OUT VARCHAR2, --返回值
                                  o_total_num OUT VARCHAR2, --记录总数
                                  o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                  ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_biz_type || '|' ||
                                    i_sys_code || '|' || i_status || '|' ||
                                    i_beg_num || '|' || i_fetch_num;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_TASK.PROC_DONE_TASK_AC_QRY';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);

    o_retcode := '0';
    IF (i_status IS NULL) THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_wf_process t4
       WHERE EXISTS (SELECT '1'
                FROM dams_wf_process_conf t3
               WHERE t3.pdid = t4.process_id
                 AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
         AND t4.complete_time IS NULL
         AND (EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2, dams_wf_participation p2
                WHERE t2.procinst_id = p2.procinst_id
                  AND t2.prev_task_id = p2.pre_task_id
                  AND t2.procinst_id = t4.procinst_id
                  AND p2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')) OR EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2
                WHERE t2.procinst_id = t4.procinst_id
                  AND t2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')))
         AND nvl(t4.business_type, 0) =
             nvl(i_biz_type, nvl(t4.business_type, 0))
         AND t4.valid_flag = '1';
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               decode(task_name, '财务受理岗', '财务受理岗', user_name),
               role_id,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               apply_name,
               apply_time,
               app_stru,
               app_stru_name,
               url,
               in_sys_seq
          FROM (SELECT task_id,
                       pckg_dams_task.func_get_cur_task(business_id) task_name,
                       decode(role_id,
                              'INNER',
                              (SELECT listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
                                 FROM dams_inner_todo_task t_open,
                                      dams_user            u1
                                WHERE t_open.business_id = tmp.business_id
                                  AND t_open.task_state = 'open'
                                  AND t_open.ssic_id = u1.ssic_id(+)),
                              pckg_dams_task.func_get_cur_dealer(business_id)) user_name,
                       role_id,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       apply_name,
                       apply_time,
                       app_stru,
                       app_stru_name,
                       url,
                       in_sys_seq
                  FROM (SELECT x.task_id,
                               x.task_name,
                               x.ssic_id,
                               x.user_name,
                               x.stru_id,
                               x.role_id,
                               x.prev_task_id,
                               x.prev_ssic_id,
                               x.prev_person_name,
                               x.prev_stru_id,
                               x.prev_role_id,
                               x.procinst_id,
                               x.process_id,
                               x.process_name,
                               x.form_id,
                               x.business_type,
                               x.business_id,
                               x.title,
                               x.create_time,
                               x.take_time,
                               x.end_time,
                               x.task_state,
                               x.outgoing_name,
                               x.opinion,
                               x.applyer,
                               x.apply_name,
                               x.apply_time,
                               x.app_stru,
                               x.app_stru_name,
                               x.url,
                               x.in_sys_seq,
                               row_number() over(ORDER BY x.create_time DESC) rn
                          FROM (SELECT to_char(t1.task_id) task_id,
                                       t1.task_name,
                                       NULL ssic_id,
                                       NULL user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       t1.create_time,
                                       t1.take_time,
                                       t1.end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       NULL url,
                                       NULL in_sys_seq
                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                                 WHERE t1.procinst_id = t4.procinst_id
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM dams_wf_process_conf t3
                                         WHERE t3.pdid = t4.process_id
                                           AND t3.sys_code =
                                               nvl(i_sys_code, t3.sys_code))
                                   AND (EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task          t2,
                                                dams_wf_participation p2
                                          WHERE t2.procinst_id = p2.procinst_id
                                            AND t2.prev_task_id = p2.pre_task_id
                                            AND t2.procinst_id = t1.procinst_id
                                            AND p2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')) OR
                                        EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task t2
                                          WHERE t2.procinst_id = t1.procinst_id
                                            AND t2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')))
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1'
                                   AND t1.task_id =
                                       (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                                          FROM dams_wf_task ttt1
                                         WHERE ttt1.procinst_id =
                                               t4.procinst_id)) x) tmp
                 WHERE rn >= to_number(i_beg_num)
                   AND rn < to_number(i_beg_num) + to_number(i_fetch_num));
    ELSIF (i_status = '1') THEN
      --未接收
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_wf_process t4
       WHERE EXISTS (SELECT '1'
                FROM dams_wf_process_conf t3
               WHERE t3.pdid = t4.process_id
                 AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
         AND t4.complete_time IS NULL
         AND (EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2, dams_wf_participation p2
                WHERE t2.procinst_id = p2.procinst_id
                  AND t2.prev_task_id = p2.pre_task_id
                  AND t2.procinst_id = t4.procinst_id
                  AND p2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')) OR EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2
                WHERE t2.procinst_id = t4.procinst_id
                  AND t2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')))
         AND nvl(t4.business_type, 0) =
             nvl(i_biz_type, nvl(t4.business_type, 0))
         AND t4.valid_flag = '1'
         AND EXISTS
       (SELECT 1
                FROM dams_wf_task t
               WHERE t.task_id =
                     (SELECT MIN(tttt1.task_id)
                        FROM dams_wf_sub_process_rel r, dams_wf_task tttt1
                       WHERE r.source_business_id = t4.business_id
                         AND r.status = '1'
                         AND r.process_type = '1'
                         AND r.business_id = tttt1.business_id)
                 AND t.take_time IS NULL);
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               decode(task_name, '财务受理岗', '财务受理岗', user_name),
               role_id,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               apply_name,
               apply_time,
               app_stru,
               app_stru_name,
               url,
               in_sys_seq
          FROM (SELECT task_id,
                       pckg_dams_task.func_get_cur_task(business_id) task_name,
                       decode(role_id,
                              'INNER',
                              (SELECT listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
                                 FROM dams_inner_todo_task t_open,
                                      dams_user            u1
                                WHERE t_open.business_id = tmp.business_id
                                  AND t_open.task_state = 'open'
                                  AND t_open.ssic_id = u1.ssic_id(+)),
                              pckg_dams_task.func_get_cur_dealer(business_id)) user_name,
                       role_id,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE',
                                                  business_type,
                                                  'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       apply_name,
                       apply_time,
                       app_stru,
                       app_stru_name,
                       url,
                       in_sys_seq
                  FROM (SELECT x.task_id,
                               x.task_name,
                               x.ssic_id,
                               x.user_name,
                               x.stru_id,
                               x.role_id,
                               x.prev_task_id,
                               x.prev_ssic_id,
                               x.prev_person_name,
                               x.prev_stru_id,
                               x.prev_role_id,
                               x.procinst_id,
                               x.process_id,
                               x.process_name,
                               x.form_id,
                               x.business_type,
                               x.business_id,
                               x.title,
                               x.create_time,
                               x.take_time,
                               x.end_time,
                               x.task_state,
                               x.outgoing_name,
                               x.opinion,
                               x.applyer,
                               x.apply_name,
                               x.apply_time,
                               x.app_stru,
                               x.app_stru_name,
                               x.url,
                               x.in_sys_seq,
                               row_number() over(ORDER BY x.create_time DESC) rn
                          FROM (SELECT to_char(t1.task_id) task_id,
                                       t1.task_name,
                                       NULL ssic_id,
                                       NULL user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       t1.create_time,
                                       t1.take_time,
                                       t1.end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       NULL url,
                                       NULL in_sys_seq
                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                                 WHERE t1.procinst_id = t4.procinst_id
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM dams_wf_process_conf t3
                                         WHERE t3.pdid = t4.process_id
                                           AND t3.sys_code =
                                               nvl(i_sys_code, t3.sys_code))
                                   AND (EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task          t2,
                                                dams_wf_participation p2
                                          WHERE t2.procinst_id = p2.procinst_id
                                            AND t2.prev_task_id = p2.pre_task_id
                                            AND t2.procinst_id = t1.procinst_id
                                            AND p2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')) OR
                                        EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task t2
                                          WHERE t2.procinst_id = t1.procinst_id
                                            AND t2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')))
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1'
                                   AND t1.task_id =
                                       (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                                          FROM dams_wf_task ttt1
                                         WHERE ttt1.procinst_id =
                                               t4.procinst_id)
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_wf_task t
                                         WHERE t.task_id =
                                               (SELECT MIN(tttt1.task_id)
                                                  FROM dams_wf_sub_process_rel r,
                                                       dams_wf_task            tttt1
                                                 WHERE r.source_business_id =
                                                       t1.business_id
                                                   AND r.status = '1'
                                                   AND r.process_type = '1'
                                                   AND r.business_id =
                                                       tttt1.business_id)
                                           AND t.take_time IS NULL)) x) tmp
                 WHERE rn >= to_number(i_beg_num)
                   AND rn < to_number(i_beg_num) + to_number(i_fetch_num));
    ELSIF (i_status = '2') THEN
      --未完成
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_wf_process t4
       WHERE EXISTS (SELECT '1'
                FROM dams_wf_process_conf t3
               WHERE t3.pdid = t4.process_id
                 AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
         AND t4.complete_time IS NULL
         AND (EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2, dams_wf_participation p2
                WHERE t2.procinst_id = p2.procinst_id
                  AND t2.prev_task_id = p2.pre_task_id
                  AND t2.procinst_id = t4.procinst_id
                  AND p2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')) OR EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2
                WHERE t2.procinst_id = t4.procinst_id
                  AND t2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')))
         AND nvl(t4.business_type, 0) =
             nvl(i_biz_type, nvl(t4.business_type, 0))
         AND t4.valid_flag = '1'
         AND EXISTS
       (SELECT 1
                FROM dams_wf_task t
               WHERE t.task_id =
                     (SELECT MIN(tttt1.task_id)
                        FROM dams_wf_sub_process_rel r,
                             dams_wf_task            tttt1,
                             ac_supervision_info     si
                       WHERE r.source_business_id = t4.business_id
                         AND r.status = '1'
                         AND r.process_type = '1'
                         AND r.business_id = tttt1.business_id
                         AND si.business_id = tttt1.business_id
                         AND (to_date(si.feedback_date, 'YYYYMMDD') - SYSDATE) >= 0)
                 AND t.take_time IS NOT NULL);
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               decode(task_name, '财务受理岗', '财务受理岗', user_name),
               role_id,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               apply_name,
               apply_time,
               app_stru,
               app_stru_name,
               url,
               in_sys_seq
          FROM (SELECT task_id,
                       pckg_dams_task.func_get_cur_task(business_id) task_name,
                       decode(role_id,
                              'INNER',
                              (SELECT listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
                                 FROM dams_inner_todo_task t_open,
                                      dams_user            u1
                                WHERE t_open.business_id = tmp.business_id
                                  AND t_open.task_state = 'open'
                                  AND t_open.ssic_id = u1.ssic_id(+)),
                              pckg_dams_task.func_get_cur_dealer(business_id)) user_name,
                       role_id,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       apply_name,
                       apply_time,
                       app_stru,
                       app_stru_name,
                       url,
                       in_sys_seq
                  FROM (SELECT x.task_id,
                               x.task_name,
                               x.ssic_id,
                               x.user_name,
                               x.stru_id,
                               x.role_id,
                               x.prev_task_id,
                               x.prev_ssic_id,
                               x.prev_person_name,
                               x.prev_stru_id,
                               x.prev_role_id,
                               x.procinst_id,
                               x.process_id,
                               x.process_name,
                               x.form_id,
                               x.business_type,
                               x.business_id,
                               x.title,
                               x.create_time,
                               x.take_time,
                               x.end_time,
                               x.task_state,
                               x.outgoing_name,
                               x.opinion,
                               x.applyer,
                               x.apply_name,
                               x.apply_time,
                               x.app_stru,
                               x.app_stru_name,
                               x.url,
                               x.in_sys_seq,
                               row_number() over(ORDER BY x.create_time DESC) rn
                          FROM (SELECT to_char(t1.task_id) task_id,
                                       t1.task_name,
                                       NULL ssic_id,
                                       NULL user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       t1.create_time,
                                       t1.take_time,
                                       t1.end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       NULL url,
                                       NULL in_sys_seq
                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                                 WHERE t1.procinst_id = t4.procinst_id
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM dams_wf_process_conf t3
                                         WHERE t3.pdid = t4.process_id
                                           AND t3.sys_code =
                                               nvl(i_sys_code, t3.sys_code))
                                   AND (EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task          t2,
                                                dams_wf_participation p2
                                          WHERE t2.procinst_id = p2.procinst_id
                                            AND t2.prev_task_id = p2.pre_task_id
                                            AND t2.procinst_id = t1.procinst_id
                                            AND p2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')) OR
                                        EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task t2
                                          WHERE t2.procinst_id = t1.procinst_id
                                            AND t2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')))
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1'
                                   AND t1.task_id =
                                       (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                                          FROM dams_wf_task ttt1
                                         WHERE ttt1.procinst_id =
                                               t4.procinst_id)
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_wf_task t
                                         WHERE t.task_id =
                                               (SELECT MIN(tttt1.task_id)
                                                  FROM dams_wf_sub_process_rel r,
                                                       dams_wf_task            tttt1,
                                                       ac_supervision_info     si
                                                 WHERE r.source_business_id =
                                                       t4.business_id
                                                   AND r.status = '1'
                                                   AND r.process_type = '1'
                                                   AND r.business_id =
                                                       tttt1.business_id
                                                   AND si.business_id =
                                                       tttt1.business_id
                                                   AND (to_date(si.feedback_date,
                                                                'YYYYMMDD') -
                                                       SYSDATE) >= 0)
                                           AND t.take_time IS NOT NULL)) x) tmp
                 WHERE rn >= to_number(i_beg_num)
                   AND rn < to_number(i_beg_num) + to_number(i_fetch_num));
    ELSIF (i_status = '3') THEN
      SELECT COUNT(1)
        INTO o_total_num
        FROM dams_wf_process t4
       WHERE EXISTS (SELECT '1'
                FROM dams_wf_process_conf t3
               WHERE t3.pdid = t4.process_id
                 AND t3.sys_code = nvl(i_sys_code, t3.sys_code))
         AND t4.complete_time IS NULL
         AND (EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2, dams_wf_participation p2
                WHERE t2.procinst_id = p2.procinst_id
                  AND t2.prev_task_id = p2.pre_task_id
                  AND t2.procinst_id = t4.procinst_id
                  AND p2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')) OR EXISTS
              (SELECT '1'
                 FROM dams_wf_task t2
                WHERE t2.procinst_id = t4.procinst_id
                  AND t2.ssic_id = i_ssic_id
                  AND t2.task_state IN ('completed')))
         AND nvl(t4.business_type, 0) =
             nvl(i_biz_type, nvl(t4.business_type, 0))
         AND t4.valid_flag = '1'
         AND EXISTS
       (SELECT r.business_id
                FROM dams_wf_sub_process_rel r,
                     dams_wf_task            tttt1,
                     ac_supervision_info     si
               WHERE r.source_business_id = t4.business_id
                 AND r.status = '1'
                 AND r.process_type = '1'
                 AND r.business_id = tttt1.business_id
                 AND si.business_id = t4.business_id
                 AND (to_date(si.feedback_date, 'YYYYMMDD') - SYSDATE) < 0);
      OPEN o_list_cur FOR
        SELECT task_id,
               task_name,
               decode(task_name, '财务受理岗', '财务受理岗', user_name),
               role_id,
               stru_id,
               prev_task_id,
               prev_ssic_id,
               prev_person_name,
               prev_stru_id,
               procinst_id,
               process_id,
               process_name,
               form_id,
               business_type,
               business_id,
               title,
               create_time,
               take_time,
               end_time,
               task_state,
               outgoing_name,
               opinion,
               apply_name,
               apply_time,
               app_stru,
               app_stru_name,
               url,
               in_sys_seq
          FROM (SELECT task_id,
                       pckg_dams_task.func_get_cur_task(business_id) task_name,
                       decode(role_id,
                              'INNER',
                              (SELECT listagg(u1.name, ',') within GROUP(ORDER BY u1.name) NAME
                                 FROM dams_inner_todo_task t_open,
                                      dams_user            u1
                                WHERE t_open.business_id = tmp.business_id
                                  AND t_open.task_state = 'open'
                                  AND t_open.ssic_id = u1.ssic_id(+)),
                              pckg_dams_task.func_get_cur_dealer(business_id)) user_name,
                       role_id,
                       stru_id,
                       prev_task_id,
                       prev_ssic_id,
                       prev_person_name,
                       prev_stru_id,
                       procinst_id,
                       process_id,
                       process_name,
                       form_id,
                       pckg_dams_util.func_decode('BIZ_TYPE', business_type, 'DAMS') business_type,
                       business_id,
                       title,
                       create_time,
                       take_time,
                       end_time,
                       task_state,
                       outgoing_name,
                       opinion,
                       apply_name,
                       apply_time,
                       app_stru,
                       app_stru_name,
                       url,
                       in_sys_seq
                  FROM (SELECT x.task_id,
                               x.task_name,
                               x.ssic_id,
                               x.user_name,
                               x.stru_id,
                               x.role_id,
                               x.prev_task_id,
                               x.prev_ssic_id,
                               x.prev_person_name,
                               x.prev_stru_id,
                               x.prev_role_id,
                               x.procinst_id,
                               x.process_id,
                               x.process_name,
                               x.form_id,
                               x.business_type,
                               x.business_id,
                               x.title,
                               x.create_time,
                               x.take_time,
                               x.end_time,
                               x.task_state,
                               x.outgoing_name,
                               x.opinion,
                               x.applyer,
                               x.apply_name,
                               x.apply_time,
                               x.app_stru,
                               x.app_stru_name,
                               x.url,
                               x.in_sys_seq,
                               row_number() over(ORDER BY x.create_time DESC) rn
                          FROM (SELECT to_char(t1.task_id) task_id,
                                       t1.task_name,
                                       NULL ssic_id,
                                       NULL user_name,
                                       t1.stru_id,
                                       t1.role_id,
                                       t1.prev_task_id,
                                       t1.prev_ssic_id,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t1.prev_ssic_id) prev_person_name,
                                       t1.prev_stru_id,
                                       t1.prev_role_id,
                                       t4.procinst_id,
                                       t4.process_id,
                                       t4.process_name,
                                       t1.form_id,
                                       t4.business_type,
                                       t4.business_id,
                                       t4.title,
                                       t1.create_time,
                                       t1.take_time,
                                       t1.end_time,
                                       t1.task_state,
                                       t1.outgoing_name,
                                       t1.opinion,
                                       t4.creator_id applyer,
                                       (SELECT u.name
                                          FROM dams_user u
                                         WHERE u.ssic_id = t4.creator_id) apply_name,
                                       to_char(t4.start_time, 'YYYY-MM-DD') apply_time,
                                       t4.creator_stru app_stru,
                                       (SELECT b.stru_sname
                                          FROM dams_branch b
                                         WHERE b.stru_id = t4.creator_stru) app_stru_name,
                                       NULL url,
                                       NULL in_sys_seq
                                  FROM dams_wf_task t1, dams_wf_process t4 --    申请任务
                                 WHERE t1.procinst_id = t4.procinst_id
                                   AND EXISTS
                                 (SELECT '1'
                                          FROM dams_wf_process_conf t3
                                         WHERE t3.pdid = t4.process_id
                                           AND t3.sys_code =
                                               nvl(i_sys_code, t3.sys_code))
                                   AND (EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task          t2,
                                                dams_wf_participation p2
                                          WHERE t2.procinst_id = p2.procinst_id
                                            AND t2.prev_task_id = p2.pre_task_id
                                            AND t2.procinst_id = t1.procinst_id
                                            AND p2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')) OR
                                        EXISTS
                                        (SELECT '1'
                                           FROM dams_wf_task t2
                                          WHERE t2.procinst_id = t1.procinst_id
                                            AND t2.ssic_id = i_ssic_id
                                            AND t2.task_state IN ('completed')))
                                   AND nvl(t4.business_type, 0) =
                                       nvl(i_biz_type,
                                           nvl(t4.business_type, 0))
                                   AND t4.valid_flag = '1'
                                   AND t1.task_id =
                                       (SELECT MAX(ttt1.task_id) keep(dense_rank FIRST ORDER BY decode(ttt1.task_state, 'open', 0, 1), ttt1.create_time DESC)
                                          FROM dams_wf_task ttt1
                                         WHERE ttt1.procinst_id =
                                               t4.procinst_id)
                                   AND EXISTS
                                 (SELECT r.business_id
                                          FROM dams_wf_sub_process_rel r,
                                               dams_wf_task            tttt1,
                                               ac_supervision_info     si
                                         WHERE r.source_business_id =
                                               t1.business_id
                                           AND r.status = '1'
                                           AND r.process_type = '1'
                                           AND r.business_id =
                                               tttt1.business_id
                                           AND si.business_id =
                                               tttt1.business_id
                                           AND (to_date(si.feedback_date,
                                                        'YYYYMMDD') - SYSDATE) < 0)) x) tmp
                 WHERE rn >= to_number(i_beg_num)
                   AND rn < to_number(i_beg_num) + to_number(i_fetch_num));
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_done_task_ac_qry;
  
  --PROCEDURE proc_task_qry_ac(i_ssic_id           IN VARCHAR2, --统一认证号
  --                           i_branch_id         IN VARCHAR2, --机构号
  --                           i_biz_type          IN VARCHAR2, --业务类型
  --                           i_sys_code          IN VARCHAR2, --系统编码
  --                           i_close_flag        IN VARCHAR2, --流程结束标志
  --                           i_applyer           IN VARCHAR2, --申请人
  --                           i_begin_date        IN VARCHAR2, --查询开始时间
  --                           i_end_date          IN VARCHAR2, --查询结束时间
  --                           i_serial_num        IN VARCHAR2, --督办编号
  --                           i_sup_title         IN VARCHAR2, --督办标题
  --                           i_sup_basis         IN VARCHAR2, --督办依据
  --                           i_cur_status        IN VARCHAR2, --当前状态
  --                           i_cb_stru_name      IN VARCHAR2, --承办单位
  --                           i_hb_stru_name      IN VARCHAR2, --会办单位
  --                           i_cb_stru_comment   IN VARCHAR2, --承办单位意见
  --                           i_hb_stru_comment   IN VARCHAR2, --会办单位意见
  --                           i_leader_comment    IN VARCHAR2, --领导意见
  --                           i_is_adopted        IN VARCHAR2, --是否采纳
  --                           i_is_overtime       IN VARCHAR2, --是否超时
  --                           i_overtime_days_min IN VARCHAR2, --超时天数最小值
  --                           i_overtime_days_max IN VARCHAR2, --超时天数最大值
  --                           i_beg_num           IN VARCHAR2, --查询开始记录
  --                           i_fetch_num         IN VARCHAR2, --查询记录数
  --                           o_retcode           OUT VARCHAR2, --返回值
  --                           o_total_num         OUT VARCHAR2, --记录总数
  --                           o_list_cur          OUT SYS_REFCURSOR --查询列表游标
  --                           ) IS
  --  v_param      db_log.info%TYPE := i_ssic_id || '|' || i_branch_id || '|' ||
  --                                   i_biz_type || '|' || i_sys_code || '|' ||
  --                                   i_close_flag || '|' || i_applyer || '|' || '|' ||
  --                                   i_begin_date || '|' || i_end_date || '|' ||
  --                                   i_beg_num || '|' || i_fetch_num || '|' ||
  --                                   i_sup_basis || '|' || i_cur_status || '|' ||
  --                                   i_cb_stru_name || '|' ||
  --                                   i_hb_stru_name || '|' ||
  --                                   i_cb_stru_comment || '|' ||
  --                                   i_hb_stru_comment || '|' ||
  --                                   i_leader_comment || '|' ||
  --                                   i_is_adopted || '|' || i_is_overtime || '|' ||
  --                                   i_overtime_days_min || '|' ||
  --                                   i_overtime_days_max;
  --  v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_TASK_BAK.PROC_TASK_QRY_AC';
  --  v_end_date   DATE;
  --  v_begin_date DATE;
  --
  --BEGIN
  --  pack_log.info(v_proc_name,
  --                pack_log.start_step,
  --                pack_log.start_msg || v_param);
  --
  --  o_retcode  := '0';
  --  v_end_date := SYSDATE;
  --  IF i_end_date IS NULL THEN
  --    NULL;
  --  ELSE
  --    v_end_date := to_date(i_end_date, 'YYYY-MM-DD') + 1;
  --  END IF;
  --
  --  IF i_begin_date IS NULL THEN
  --    v_begin_date := to_date('1900-01-01', 'YYYY-MM-DD');
  --  ELSE
  --    v_begin_date := to_date(i_begin_date, 'YYYY-MM-DD');
  --  END IF;
  --
  --  SELECT COUNT(1)
  --    INTO o_total_num
  --    FROM ac_supervision_info aa, ac_supervision_chb_unit bb
  --   WHERE aa.business_id IN
  --         (SELECT a.business_id
  --            FROM ac_supervision_info a
  --           WHERE a.source_business_id IS NULL)
  --     AND (EXISTS (SELECT 1
  --                    FROM dams_branch_relation tt
  --                   WHERE tt.up_bnch_id IN
  --                         (SELECT rr.stru_id
  --                            FROM dams_user_role_rel rr
  --                           WHERE rr.role_id = 'AC099'
  --                             AND rr.ssic_id = i_ssic_id
  --                             AND rr.auth_state IN ('0', '2'))
  --                     AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --          (SELECT 1
  --             FROM dams_branch_relation tt
  --            WHERE tt.up_bnch_id IN
  --                  (SELECT t.major_stru_id
  --                     FROM dams_branch_catalog t
  --                    WHERE t.stru_id IN
  --                          (SELECT rr.stru_id
  --                             FROM dams_user_role_rel rr
  --                            WHERE rr.role_id = 'AC014'
  --                              AND rr.ssic_id = i_ssic_id
  --                              AND rr.auth_state IN ('0', '2')))
  --              AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --          (SELECT 1
  --             FROM dams_branch_relation tt
  --            WHERE tt.up_bnch_id IN
  --                  (SELECT t.dept_stru_id
  --                     FROM dams_branch_catalog t
  --                    WHERE t.stru_id IN
  --                          (SELECT rr.stru_id
  --                             FROM dams_user_role_rel rr
  --                            WHERE rr.role_id = 'AC004'
  --                              AND rr.ssic_id = i_ssic_id
  --                              AND rr.auth_state IN ('0', '2')))
  --              AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --          (SELECT 1
  --             FROM dams_branch_catalog tt
  --            WHERE tt.major_stru_id IN
  --                  (SELECT t.major_stru_id
  --                     FROM dams_branch_catalog t
  --                    WHERE t.stru_id IN
  --                          (SELECT rr.stru_id
  --                             FROM dams_user_role_rel rr
  --                            WHERE rr.role_id IN ('AC007', 'AC006')
  --                              AND rr.ssic_id = i_ssic_id
  --                              AND rr.auth_state IN ('0', '2')))
  --              AND tt.stru_id = aa.drafter_unit_id) OR EXISTS
  --          (SELECT 1
  --             FROM dams_branch_catalog tt
  --            WHERE tt.dept_stru_id IN
  --                  (SELECT t.dept_stru_id
  --                     FROM dams_branch_catalog t
  --                    WHERE t.stru_id IN
  --                          (SELECT rr.stru_id
  --                             FROM dams_user_role_rel rr
  --                            WHERE rr.role_id IN ('AC005',
  --                                                 'AC000',
  --                                                 'AC002',
  --                                                 'AC003',
  --                                                 'AC012',
  --                                                 'AC018',
  --                                                 'AC039')
  --                              AND rr.ssic_id = i_ssic_id
  --                              AND rr.auth_state IN ('0', '2')))
  --              AND tt.stru_id = aa.drafter_unit_id) OR EXISTS
  --          (SELECT 1
  --             FROM dams_branch_catalog tt
  --            WHERE tt.stru_id IN
  --                  (SELECT rr.stru_id
  --                     FROM dams_user_role_rel rr
  --                    WHERE rr.role_id IN ('AC010')
  --                      AND rr.ssic_id = i_ssic_id
  --                      AND rr.auth_state IN ('0', '2'))
  --              AND tt.stru_id = aa.drafter_unit_id))
  --     AND (i_applyer IS NULL OR
  --         aa.drafter_ssic_id IN
  --         (SELECT t.ssic_id FROM dams_user t WHERE t.name = i_applyer))
  --     AND aa.app_time >= v_begin_date
  --     AND aa.app_time < v_end_date
  --     AND (i_cb_stru_name IS NULL OR EXISTS
  --          (SELECT 1
  --             FROM ac_supervision_chb_unit cu, dams_branch tb
  --            WHERE cu.business_id = aa.business_id
  --              AND cu.stru_id = tb.stru_id
  --              AND cu.type = '1'
  --              AND tb.stru_sname LIKE '%' || i_cb_stru_name || '%'))
  --     AND (i_hb_stru_name IS NULL OR EXISTS
  --          (SELECT 1
  --             FROM ac_supervision_chb_unit cu, dams_branch tb
  --            WHERE cu.business_id = aa.business_id
  --              AND cu.stru_id = tb.stru_id
  --              AND cu.type = '2'
  --              AND tb.stru_sname LIKE '%' || i_hb_stru_name || '%'))
  --     AND (i_cb_stru_comment IS NULL OR EXISTS
  --          (SELECT 1
  --             FROM ac_supervision_chb_unit cu
  --            WHERE cu.business_id = aa.business_id
  --              AND cu.type = '1'
  --              AND cu.chb_comment LIKE '%' || i_cb_stru_comment || '%'))
  --     AND (i_hb_stru_comment IS NULL OR EXISTS
  --          (SELECT 1
  --             FROM ac_supervision_chb_unit cu
  --            WHERE cu.business_id = aa.business_id
  --              AND cu.type = '2'
  --              AND cu.chb_comment LIKE '%' || i_hb_stru_comment || '%'))
  --     AND (i_leader_comment IS NULL OR EXISTS
  --          (SELECT 1
  --             FROM ac_supervision_leader_comment sc
  --            WHERE sc.group_id = aa.business_id
  --              AND sc.leader_comment LIKE '%' || i_leader_comment || '%'))
  --     AND (i_serial_num IS NULL OR
  --         aa.serial_num LIKE '%' || i_serial_num || '%')
  --     AND (i_sup_title IS NULL OR
  --         aa.sup_title LIKE '%' || i_sup_title || '%')
  --     AND (i_sup_basis IS NULL OR
  --         aa.sup_basis LIKE '%' || i_sup_basis || '%')
  --     AND (i_is_adopted IS NULL OR aa.is_adopt = i_is_adopted)
  --     AND (i_cur_status IS NULL OR
  --         i_cur_status = decode((SELECT dp.complete_time
  --                                  FROM viw_dams_wf_all_process dp
  --                                 WHERE dp.business_id = aa.business_id),
  --                                NULL,
  --                                '1',
  --                                '2'))
  --     AND bb.business_id = aa.business_id
  --     AND bb.type = '1'
  --     AND (i_is_overtime IS NULL OR
  --         (i_is_overtime = decode(decode(bb.feedback_time,  NULL, pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id, aa.feedback_date), bb.ot_days), 0, '0', '1')))
  --     AND (i_overtime_days_min IS NULL OR i_overtime_days_min <= pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id, aa.feedback_date))
  --     AND (i_overtime_days_max IS NULL OR i_overtime_days_max >= pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id, aa.feedback_date));
  --
  --   OPEN o_list_cur FOR
  --    SELECT task_id,
  --           wt.task_name,
  --           user_name,
  --           tmp.business_id,
  --           title,
  --           apply_time,
  --           business_type
  --      FROM (SELECT pckg_dams_task.func_get_cur_task(aa.business_id) task_name,
  --                   pckg_dams_task.func_get_cur_dealer(aa.business_id) user_name,
  --                   aa.business_id,
  --                   aa.sup_title title,
  --                   aa.app_time apply_time,
  --                   pckg_dams_util.func_decode('BIZ_TYPE', 'AC01', 'DAMS') business_type,
  --                   dense_rank() over(ORDER BY aa.app_time DESC, aa.business_id DESC) rn
  --              FROM ac_supervision_info aa, ac_supervision_chb_unit bb
  --             WHERE aa.business_id IN
  --                   (SELECT a.business_id
  --                      FROM ac_supervision_info a
  --                     WHERE a.source_business_id IS NULL)
  --               AND (EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_relation tt
  --                      WHERE tt.up_bnch_id IN
  --                            (SELECT rr.stru_id
  --                               FROM dams_user_role_rel rr
  --                              WHERE rr.role_id = 'AC099'
  --                                AND rr.ssic_id = i_ssic_id
  --                                AND rr.auth_state IN ('0', '2'))
  --                        AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_relation tt
  --                      WHERE tt.up_bnch_id IN
  --                            (SELECT t.major_stru_id
  --                               FROM dams_branch_catalog t
  --                              WHERE t.stru_id IN
  --                                    (SELECT rr.stru_id
  --                                       FROM dams_user_role_rel rr
  --                                      WHERE rr.role_id = 'AC014'
  --                                        AND rr.ssic_id = i_ssic_id
  --                                        AND rr.auth_state IN ('0', '2')))
  --                        AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_relation tt
  --                      WHERE tt.up_bnch_id IN
  --                            (SELECT t.dept_stru_id
  --                               FROM dams_branch_catalog t
  --                              WHERE t.stru_id IN
  --                                    (SELECT rr.stru_id
  --                                       FROM dams_user_role_rel rr
  --                                      WHERE rr.role_id = 'AC004'
  --                                        AND rr.ssic_id = i_ssic_id
  --                                        AND rr.auth_state IN ('0', '2')))
  --                        AND tt.bnch_id = aa.drafter_unit_id) OR EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_catalog tt
  --                      WHERE tt.major_stru_id IN
  --                            (SELECT t.major_stru_id
  --                               FROM dams_branch_catalog t
  --                              WHERE t.stru_id IN
  --                                    (SELECT rr.stru_id
  --                                       FROM dams_user_role_rel rr
  --                                      WHERE rr.role_id IN ('AC007', 'AC006')
  --                                        AND rr.ssic_id = i_ssic_id
  --                                        AND rr.auth_state IN ('0', '2')))
  --                        AND tt.stru_id = aa.drafter_unit_id) OR EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_catalog tt
  --                      WHERE tt.dept_stru_id IN
  --                            (SELECT t.dept_stru_id
  --                               FROM dams_branch_catalog t
  --                              WHERE t.stru_id IN
  --                                    (SELECT rr.stru_id
  --                                       FROM dams_user_role_rel rr
  --                                      WHERE rr.role_id IN
  --                                            ('AC005', 'AC000', 'AC002', 'AC003',
  --                                             'AC012', 'AC018', 'AC039')
  --                                        AND rr.ssic_id = i_ssic_id
  --                                        AND rr.auth_state IN ('0', '2')))
  --                        AND tt.stru_id = aa.drafter_unit_id) OR EXISTS
  --                    (SELECT 1
  --                       FROM dams_branch_catalog tt
  --                      WHERE tt.stru_id IN
  --                            (SELECT rr.stru_id
  --                               FROM dams_user_role_rel rr
  --                              WHERE rr.role_id IN ('AC010')
  --                                AND rr.ssic_id = i_ssic_id
  --                                AND rr.auth_state IN ('0', '2'))
  --                        AND tt.stru_id = aa.drafter_unit_id))
  --               AND (i_applyer IS NULL OR
  --                   aa.drafter_ssic_id IN
  --                   (SELECT t.ssic_id FROM dams_user t WHERE t.name = i_applyer))
  --               AND aa.app_time >= v_begin_date
  --               AND aa.app_time < v_end_date
  --               AND (i_cb_stru_name IS NULL OR EXISTS
  --                    (SELECT 1
  --                       FROM ac_supervision_chb_unit cu, dams_branch tb
  --                      WHERE cu.business_id = aa.business_id
  --                        AND cu.stru_id = tb.stru_id
  --                        AND cu.type = '1'
  --                        AND tb.stru_sname LIKE '%' || i_cb_stru_name || '%'))
  --               AND (i_hb_stru_name IS NULL OR EXISTS
  --                    (SELECT 1
  --                       FROM ac_supervision_chb_unit cu, dams_branch tb
  --                      WHERE cu.business_id = aa.business_id
  --                        AND cu.stru_id = tb.stru_id
  --                        AND cu.type = '2'
  --                        AND tb.stru_sname LIKE '%' || i_hb_stru_name || '%'))
  --               AND (i_cb_stru_comment IS NULL OR EXISTS
  --                    (SELECT 1
  --                       FROM ac_supervision_chb_unit cu
  --                      WHERE cu.business_id = aa.business_id
  --                        AND cu.type = '1'
  --                        AND cu.chb_comment LIKE '%' || i_cb_stru_comment || '%'))
  --               AND (i_hb_stru_comment IS NULL OR EXISTS
  --                    (SELECT 1
  --                       FROM ac_supervision_chb_unit cu
  --                      WHERE cu.business_id = aa.business_id
  --                        AND cu.type = '2'
  --                        AND cu.chb_comment LIKE '%' || i_hb_stru_comment || '%'))
  --               AND (i_leader_comment IS NULL OR EXISTS
  --                    (SELECT 1
  --                       FROM ac_supervision_leader_comment sc
  --                      WHERE sc.group_id = aa.business_id
  --                        AND sc.leader_comment LIKE
  --                            '%' || i_leader_comment || '%'))
  --               AND (i_serial_num IS NULL OR
  --                   aa.serial_num LIKE '%' || i_serial_num || '%')
  --               AND (i_sup_title IS NULL OR
  --                   aa.sup_title LIKE '%' || i_sup_title || '%')
  --               AND (i_sup_basis IS NULL OR
  --                   aa.sup_basis LIKE '%' || i_sup_basis || '%')
  --               AND (i_is_adopted IS NULL OR aa.is_adopt = i_is_adopted)
  --               AND (i_cur_status IS NULL OR
  --                   i_cur_status =
  --                   decode((SELECT dp.complete_time FROM viw_dams_wf_all_process dp  WHERE dp.business_id = aa.business_id), NULL, '1', '2'))
  --               AND bb.business_id = aa.business_id
  --               AND bb.type = '1'
  --               AND (i_is_overtime IS NULL OR
  --                   (i_is_overtime =
  --                   decode(decode(bb.feedback_time, NULL, pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id, aa.feedback_date), bb.ot_days), 0, '0', '1')))
  --               AND (i_overtime_days_min IS NULL OR
  --                   i_overtime_days_min <=
  --                   pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id,
  --                                                        aa.feedback_date))
  --               AND (i_overtime_days_max IS NULL OR
  --                   i_overtime_days_max >=
  --                   pckg_ac_supervision.fun_cal_ot_days(aa.drafter_unit_id, aa.feedback_date))) tmp,
  --           viw_dams_wf_all_task wt
  --     WHERE wt.task_id = (SELECT MAX(t.task_id) keep(dense_rank FIRST ORDER BY t.create_time DESC)
  --                           FROM viw_dams_wf_all_task t
  --                          WHERE t.business_id = tmp.business_id)
  --       AND tmp.rn >= to_number(i_beg_num)
  --       AND tmp.rn < to_number(i_beg_num) + to_number(i_fetch_num);
  --  pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --    o_retcode := '-1';
  --    ROLLBACK;
  --    RETURN;
  --END proc_task_qry_ac;
  
END pckg_dams_task;
//


CREATE OR REPLACE PACKAGE pckg_dams_branch IS
  TYPE ret_list IS REF CURSOR;
  TYPE arrytype IS TABLE OF dams_branch.stru_id%TYPE;
  PROCEDURE proc_branch_tree(i_id     IN VARCHAR2, --机构编码
                             i_type   IN VARCHAR2, --类型，为空表示默认出树逻辑
                             i_userid IN VARCHAR2,
                             i_branch IN VARCHAR2, --根节点编码
                             --i_showBch IN VARCHAR2, --显示根节点编码
                             --kfzx-guoq03                         i_system      IN VARCHAR2, --系统编码
                             o_flag        OUT VARCHAR2, --存储过程返回标志
                             o_branch_list OUT ret_list --角色列表
                             );
   
   PROCEDURE proc_branch_update(i_sourceid IN VARCHAR2, --机构编码
                               i_targetid IN VARCHAR2, --目标父机构编码
                               --kfzx-guoq03                               i_syscode  IN VARCHAR2,
                               i_user IN VARCHAR2, --操作人
                               o_flag OUT VARCHAR2, --存储过程返回标志
                               o_msg  OUT VARCHAR2 --角色列表
                               );
    
	PROCEDURE proc_branch_info(i_id IN VARCHAR2, --机构编码
	                         o_id     OUT VARCHAR2, --机构id
                             o_fname  OUT VARCHAR2, --机构名称
                             o_sname  OUT VARCHAR2, --机构名称
                             o_parent OUT VARCHAR2, --父机构名称
                             o_level  OUT VARCHAR2, --层级
                             o_flag   OUT VARCHAR2);
	
	PROCEDURE pckg_get_stru_by_name(i_stru_name IN VARCHAR2, --关键字
                                  i_shownum   IN VARCHAR2,
                                  i_branch_id IN VARCHAR2, --根节点机构id
                                  i_type      IN VARCHAR2, --0全部 1分支机构树（符合ST）
                                  o_ret_code  OUT VARCHAR2,
                                  o_list      OUT SYS_REFCURSOR);
	
END pckg_dams_branch;
//


CREATE OR REPLACE PACKAGE BODY pckg_dams_branch IS
  PROCEDURE proc_branch_tree(i_id     IN VARCHAR2, --展开机构编码
                             i_type   IN VARCHAR2, --类型，为空表示默认出树逻辑
                             i_userid IN VARCHAR2,
                             i_branch IN VARCHAR2, --根节点编码
                             --i_showBch IN VARCHAR2, --显示根节点编码
                             --kfzx-guoq03                         i_system      IN VARCHAR2, --系统编码
                             o_flag        OUT VARCHAR2, --存储过程返回标志
                             o_branch_list OUT ret_list --角色列表
                             ) IS
    struarray arrytype;
    TYPE array_type IS VARRAY(50) OF NUMBER;
    codevar array_type;
    v_tempstru  VARCHAR2(4000);
    v_inusestru VARCHAR2(4000) := '';
    v_flag      NUMBER := 1;
  BEGIN
    o_flag  := '0';
    codevar := array_type();
    --默认出树逻辑
    IF i_type IS NULL THEN
      SELECT DISTINCT t.stru_id BULK COLLECT
        INTO struarray
        FROM dams_user_role_rel t, dams_role r
       WHERE t.role_id = r.role_id
         AND r.status = '1'
         AND instr(r.role_type, 'ADMIN') > 0
         AND t.ssic_id = i_userid;
      FOR i IN 1 .. struarray.count
      LOOP
        FOR j IN 1 .. struarray.count
        LOOP
          IF j != i THEN
            SELECT listagg(t.stru_id, ',')
              INTO v_tempstru
              FROM dams_branch t
             WHERE t.stru_id != struarray(i)
             START WITH t.stru_id = struarray(i)
            CONNECT BY PRIOR t.sup_stru = t.stru_id;
            IF instr(v_tempstru, struarray(j)) > 0 THEN
              codevar.extend();
              codevar(v_flag) := i;
              v_flag := v_flag + 1;
              EXIT;
            END IF;
          END IF;
        END LOOP;
      END LOOP;
      IF codevar IS NOT NULL THEN
        FOR k IN 1 .. struarray.count
        LOOP
          FOR l IN 1 .. codevar.count
          LOOP
            IF codevar(l) IS NOT NULL AND k = codevar(l) THEN
              struarray(k) := '';
            END IF;
          END LOOP;
        END LOOP;
      END IF;
      FOR m IN 1 .. struarray.count
      LOOP
        v_inusestru := v_inusestru || ',' || struarray(m);
      END LOOP;
      v_inusestru := ',' || v_inusestru || ',';
      --未指定i_id,初始化树逻辑
      IF i_id = '0' OR i_id IS NULL OR instr(i_id, '$', 1) > 0 THEN
        --不指定根节点，通过角色管理机构决定根节点
        IF i_branch IS NULL THEN
          OPEN o_branch_list FOR
            SELECT b.stru_id,
                            b.stru_sname,
                            CASE
                              WHEN (SELECT 1
                                      FROM dual
                                     WHERE EXISTS
                                     (SELECT 1
                                              FROM dams_branch
                                             WHERE sup_stru = b.stru_id
                                               AND nvl(stru_lv, '0') NOT IN
                                                   ('73', '74', '75', '76', '77',
                                                    '79', '80')
                                               AND stru_sign <> '6'
                                               AND stru_sname NOT LIKE '%删除%'
                                               AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                               '0'
                              ELSE
                               '1'
                            END isleaf, --是否有叶节点
                            '' sup_stru
              FROM dams_branch b
             WHERE instr(v_inusestru, ',' || b.stru_id || ',') > 0
                  --modify by haozj refer to 网讯通讯录
               AND nvl(stru_lv, '0') NOT IN
                   ('73', '74', '75', '76', '77', '79', '80')
               AND stru_sign <> '6'
               AND stru_sname NOT LIKE '%删除%'
               AND stru_state IN ('1', '2')
             ORDER BY b.stru_sort DESC NULLS LAST,
                      b.sort DESC NULLS LAST,
                      b.stru_sname;
        ELSE
          --指定根节点
          OPEN o_branch_list FOR
            SELECT b.stru_id,
                   b.stru_sname,
                   CASE
                     WHEN (SELECT 1
                             FROM dual
                            WHERE EXISTS
                            (SELECT 1
                                     FROM dams_branch
                                    WHERE sup_stru = b.stru_id
                                      AND nvl(stru_lv, '0') NOT IN
                                          ('73', '74', '75', '76', '77', '79', '80')
                                      AND stru_sign <> '6'
                                      AND stru_sname NOT LIKE '%删除%'
                                      AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                      '0'
                     ELSE
                      '1'
                   END isleaf, --是否有叶节点
                   '' sup_stru
              FROM dams_branch b
             WHERE b.stru_id = i_branch
                  --modify by haozj refer to 网讯通讯录
               AND nvl(b.stru_lv, '0') NOT IN
                   ('73', '74', '75', '76', '77', '79', '80')
               AND b.stru_sign <> '6'
               AND b.stru_sname NOT LIKE '%删除%'
               AND b.stru_state IN ('1', '2')
             ORDER BY b.stru_sort DESC NULLS LAST,
                      b.sort DESC NULLS LAST,
                      b.stru_sname;
        END IF;
      ELSE
        --指定i_id，点击展开时异步获取子树
        OPEN o_branch_list FOR
          SELECT stru_id,
                 stru_sname,
                 CASE
                   WHEN (SELECT 1
                           FROM dual
                          WHERE EXISTS
                          (SELECT 1
                                   FROM dams_branch
                                  WHERE sup_stru = b.stru_id
                                    AND nvl(stru_lv, '0') NOT IN
                                        ('73', '74', '75', '76', '77', '79', '80')
                                    AND stru_sign <> '6'
                                    AND stru_sname NOT LIKE '%删除%'
                                    AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                    '0'
                   ELSE
                    '1'
                 END isleaf, --是否有叶节点
                 sup_stru
            FROM dams_branch b
           WHERE b.sup_stru = i_id
                --modify by haozj refer to 网讯通讯录
             AND nvl(b.stru_lv, '0') NOT IN
                 ('73', '74', '75', '76', '77', '79', '80')
             AND b.stru_sign <> '6'
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2')
           ORDER BY b.stru_sort DESC NULLS LAST,
                    b.sort DESC NULLS LAST,
                    b.stru_sname;
      END IF;
    ELSE
      CASE i_type
      --保密系统机构树
        WHEN 'ST' THEN
          --未指定i_id,初始化树逻辑
          IF i_id = '0' OR i_id IS NULL OR instr(i_id, '$', 1) > 0 THEN
            --不指定根节点，通过角色管理机构决定根节点
            IF i_branch IS NULL THEN
              --通过角色计算根节点列表
              --SELECT dams_conn(stru_id)
              SELECT stru_id
                INTO v_inusestru
                FROM (SELECT up_bnch_id stru_id
                        FROM (SELECT t1.bnch_id,
                                     t1.up_bnch_id,
                                     t1.up_level,
                                     MIN(t1.up_level) over(PARTITION BY t.stru_id) min_level
                                FROM dams_user_role_rel   t,
                                     dams_branch_relation t1
                               WHERE t.role_id = 'DR20000029'
                                 AND t.ssic_id = i_userid
                                 AND t.stru_id = t1.bnch_id
                                 AND (t1.up_bnch_sign = '1' AND
                                     t1.up_bnch_grade IN ('1', '2', '3', '4') OR
                                     t1.up_bnch_sign = '4' OR
                                     t1.up_bnch_sign IN ('8', '18') AND
                                     (SELECT b.man_grade
                                         FROM dams_branch b
                                        WHERE b.stru_id = t1.up_bnch_id) = '1'))
                       WHERE up_level = min_level
                      MINUS
                      SELECT DISTINCT stru_id
                        FROM (SELECT a1.stru_id
                                FROM (SELECT up_bnch_id stru_id
                                        FROM (SELECT t1.bnch_id,
                                                     t1.up_bnch_id,
                                                     t1.up_level,
                                                     MIN(t1.up_level) over(PARTITION BY t.stru_id) min_level
                                                FROM dams_user_role_rel   t,
                                                     dams_branch_relation t1
                                               WHERE t.role_id = 'DR20000029'
                                                 AND t.ssic_id = i_userid
                                                 AND t.stru_id = t1.bnch_id
                                                 AND (t1.up_bnch_sign = '1' AND
                                                     t1.up_bnch_grade IN
                                                     ('1', '2', '3', '4') OR
                                                     t1.up_bnch_sign = '4' OR
                                                     t1.up_bnch_sign IN
                                                     ('8', '18') AND
                                                     (SELECT b.man_grade
                                                         FROM dams_branch b
                                                        WHERE b.stru_id =
                                                              t1.up_bnch_id) = '1'))
                                       WHERE up_level = min_level) a1,
                                     (SELECT up_bnch_id stru_id
                                        FROM (SELECT t1.bnch_id,
                                                     t1.up_bnch_id,
                                                     t1.up_level,
                                                     MIN(t1.up_level) over(PARTITION BY t.stru_id) min_level
                                                FROM dams_user_role_rel   t,
                                                     dams_branch_relation t1
                                               WHERE t.role_id = 'DR20000029'
                                                 AND t.ssic_id = i_userid
                                                 AND t.stru_id = t1.bnch_id
                                                 AND (t1.up_bnch_sign = '1' AND
                                                     t1.up_bnch_grade IN
                                                     ('1', '2', '3', '4') OR
                                                     t1.up_bnch_sign = '4' OR
                                                     t1.up_bnch_sign IN
                                                     ('8', '18') AND
                                                     (SELECT b.man_grade
                                                         FROM dams_branch b
                                                        WHERE b.stru_id =
                                                              t1.up_bnch_id) = '1'))
                                       WHERE up_level = min_level) a2,
                                     dams_branch_relation br
                               WHERE br.bnch_id = a1.stru_id
                                 AND br.up_bnch_id = a2.stru_id
                                 AND a1.stru_id != a2.stru_id));
              v_inusestru := ',' || v_inusestru || ',';
              --计算树
              OPEN o_branch_list FOR
                SELECT b.stru_id,
                                b.stru_sname,
                                CASE
                                  WHEN (SELECT 1
                                          FROM dual
                                         WHERE EXISTS
                                         (SELECT 1
                                                  FROM dams_branch
                                                 WHERE sup_stru = b.stru_id
                                                   AND (stru_sign = '1' AND
                                                       stru_grade IN
                                                       ('1', '2', '3', '4') OR
                                                       stru_sign IN ('8', '18') AND
                                                       man_grade = '1' OR
                                                       stru_sign = '4' AND
                                                       man_grade IN ('1', '2'))
                                                   AND stru_sname NOT LIKE '%删除%'
                                                   AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                                   '0'
                                  ELSE
                                   '1'
                                END isleaf, --是否有叶节点
                                '' sup_stru
                  FROM dams_branch b
                 WHERE instr(v_inusestru, ',' || b.stru_id || ',') > 0
                   AND (b.stru_sign = '1' AND
                        b.stru_grade IN ('1', '2', '3', '4') OR
                        b.stru_sign IN ('8', '18') AND b.man_grade = '1' OR
                        b.stru_sign = '4' AND b.man_grade IN ('1', '2'))
                   AND b.stru_sname NOT LIKE '%删除%'
                   AND b.stru_state IN ('1', '2')
                 ORDER BY b.stru_sort DESC NULLS LAST,
                          b.sort DESC NULLS LAST,
                          b.stru_sname;
            ELSE
              --指定根节点
              OPEN o_branch_list FOR
                SELECT b.stru_id,
                       b.stru_sname,
                       CASE
                         WHEN (SELECT 1
                                 FROM dual
                                WHERE EXISTS
                                (SELECT 1
                                         FROM dams_branch
                                        WHERE sup_stru = b.stru_id
                                          AND nvl(stru_lv, '0') NOT IN
                                              ('73', '74', '75', '76', '77', '79',
                                               '80')
                                          AND (stru_sign = '1' AND
                                              stru_grade IN ('1', '2', '3', '4') OR
                                              stru_sign IN ('8', '18') AND
                                              man_grade = '1' OR
                                              stru_sign = '4' AND
                                              man_grade IN ('1', '2'))
                                          AND stru_sname NOT LIKE '%删除%'
                                          AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                          '0'
                         ELSE
                          '1'
                       END isleaf, --是否有叶节点
                       '' sup_stru
                  FROM dams_branch b
                 WHERE b.stru_id = i_branch
                   AND (b.stru_sign = '1' AND
                       b.stru_grade IN ('1', '2', '3', '4') OR
                       b.stru_sign IN ('8', '18') AND b.man_grade = '1' OR
                       b.stru_sign = '4' AND b.man_grade IN ('1', '2'))
                   AND b.stru_sname NOT LIKE '%删除%'
                   AND b.stru_state IN ('1', '2')
                 ORDER BY b.stru_sort DESC NULLS LAST,
                          b.sort DESC NULLS LAST,
                          b.stru_sname;
                          --nlssort(b.stru_sname, 'NLS_SORT=SCHINESE_PINYIN_M');
            END IF;
          ELSE
            --指定i_id，点击展开时异步获取子树
            OPEN o_branch_list FOR
              SELECT stru_id,
                     stru_sname,
                     CASE
                       WHEN (SELECT 1
                               FROM dual
                              WHERE EXISTS
                              (SELECT 1
                                       FROM dams_branch
                                      WHERE sup_stru = b.stru_id
                                        AND nvl(stru_lv, '0') NOT IN
                                            ('73', '74', '75', '76', '77', '79',
                                             '80')
                                        AND (stru_sign = '1' AND
                                            stru_grade IN ('1', '2', '3', '4') OR
                                            stru_sign IN ('8', '18') AND
                                            man_grade = '1' OR
                                            stru_sign = '4' AND
                                            man_grade IN ('1', '2'))
                                        AND stru_sname NOT LIKE '%删除%'
                                        AND stru_state IN ('1', '2'))) IS NOT NULL THEN
                        '0'
                       ELSE
                        '1'
                     END isleaf, --是否有叶节点
                     sup_stru
                FROM dams_branch b
               WHERE b.sup_stru = i_id
                 AND (b.stru_sign = '1' AND
                     b.stru_grade IN ('1', '2', '3', '4') OR
                     b.stru_sign IN ('8', '18') AND b.man_grade = '1' OR
                     b.stru_sign = '4' AND b.man_grade IN ('1', '2'))
                 AND b.stru_sname NOT LIKE '%删除%'
                 AND b.stru_state IN ('1', '2')
               ORDER BY b.stru_sort DESC NULLS LAST,
                        b.sort DESC NULLS LAST,
                        b.stru_sname;
          END IF;
      END CASE;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      IF o_branch_list%ISOPEN THEN
        CLOSE o_branch_list;
      END IF;
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_branch_tree;
  
  PROCEDURE proc_branch_update(i_sourceid IN VARCHAR2, --机构编码
                               i_targetid IN VARCHAR2, --目标父机构编码
                               --kfzx-guoq03                               i_syscode  IN VARCHAR2,
                               i_user IN VARCHAR2, --操作人
                               o_flag OUT VARCHAR2, --存储过程返回标志
                               o_msg  OUT VARCHAR2 --角色列表
                               ) IS
    v_1 VARCHAR2(20);
    v_2 VARCHAR2(20);
    v_3 VARCHAR2(20);
  BEGIN
    v_1 := to_char(SYSDATE, 'yyyymmdd hh24miss');
    DELETE dams_branch_rel rel WHERE rel.stru_id = i_sourceid; --鍒犻櫎鍘熷叧绯?
    v_2 := to_char(SYSDATE, 'yyyymmdd hh24miss');
    --INSERT INTO dams_branch_rel rel (stru_id, sup_stru) VALUES(i_sourceid, i_targetid);
    INSERT INTO dams_branch_rel rel (stru_id, sup_stru, sys_code) VALUES(i_sourceid, i_targetid, i_user);
    COMMIT;
    v_3 := to_char(SYSDATE, 'yyyymmdd hh24miss');
    --INSERT INTO test VALUES (v_1, v_2, v_3);
    o_flag := '0';
    o_msg  := '修改成功';
  EXCEPTION
    WHEN OTHERS THEN
      --log('ctp_proc_role_getrolelist()',SQLERRM(SQLCODE));
      o_flag := '-1';
      RETURN;
  END proc_branch_update;
  
  PROCEDURE proc_branch_info(i_id IN VARCHAR2, --机构编码
                             o_id     OUT VARCHAR2, --机构id
                             o_fname  OUT VARCHAR2, --机构名称
                             o_sname  OUT VARCHAR2, --机构名称
                             o_parent OUT VARCHAR2, --父机构名称
                             o_level  OUT VARCHAR2, --层级
                             o_flag   OUT VARCHAR2) IS
  BEGIN
    o_flag := '0';
    SELECT b.stru_id,
           b.stru_fname,
           b.stru_sname,
           (SELECT stru_fname FROM dams_branch WHERE stru_id = b.sup_stru),
           d.dictvalue
      INTO o_id,
           o_fname, --机构全称
           o_sname, --机构简称
           o_parent, --上级机构
           o_level
      FROM dams_branch b, dams_gen_dict d
     WHERE b.stru_id = i_id
       AND b.stru_lv = d.dictcode(+)
       AND d.dicttype(+) = 'STRU_LV'
       AND d.sys_code(+) = 'DAMS'
       AND d.dictlang = 'zh_CN';
  END proc_branch_info;
  
  PROCEDURE pckg_get_stru_by_name(i_stru_name IN VARCHAR2, --关键字
                                  i_shownum   IN VARCHAR2,
                                  i_branch_id IN VARCHAR2, --根节点机构id
                                  i_type      IN VARCHAR2, --0全部 1分支机构树（符合ST）
                                  o_ret_code  OUT VARCHAR2,
                                  o_list      OUT SYS_REFCURSOR) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_branch.pckg_get_stru_by_name';
    v_param     db_log.info%TYPE := i_stru_name || ',' || i_shownum || ',' ||
                                    i_branch_id || ',' || i_type;
    v_step      db_log.step_no%TYPE;
    v_stru      dams_branch.stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    o_ret_code := '0';
    IF (i_type = '1') THEN
      v_stru := pckg_dams_util.func_get_lv1_branch(i_branch_id);
      --只有分支机构和其他
      OPEN o_list FOR
        SELECT stru_id VALUE, stru_sname text
          FROM (SELECT b.stru_id, b.stru_sname
                  FROM dams_branch_relation r, dams_branch b
                 WHERE r.bnch_id = b.stru_id
                   AND r.up_bnch_id = v_stru
                   AND b.stru_sname LIKE '%' || i_stru_name || '%'
                   AND b.stru_sign IN ('1', '4', '8', '18')
                 ORDER BY b.man_grade)
         WHERE rownum <= i_shownum;
    ELSIF (i_type = '2') THEN
      v_stru := pckg_dams_util.func_get_lv1_branch(i_branch_id);
      IF v_stru = '0010100000' THEN
        --只有总行包含总行本部内部室
        OPEN o_list FOR
          SELECT stru_id VALUE, stru_sname text
            FROM (SELECT b.stru_id, b.stru_sname, b.man_grade
                    FROM dams_branch_relation r, dams_branch b
                   WHERE r.bnch_id = b.stru_id
                     AND r.up_bnch_id = v_stru
                     AND b.stru_sname LIKE '%' || i_stru_name || '%'
                     AND b.stru_sign IN ('1', '4', '8', '18')
                  UNION ALL
                  SELECT b.stru_id, b.stru_sname, b.man_grade
                    FROM dams_branch b
                   WHERE b.sup_stru = '0010100500'
                     AND b.stru_state IN ('1', '2')
                     AND b.stru_sign IN ('3', '4')
                     AND b.stru_sname LIKE '%' || i_stru_name || '%'
                   ORDER BY man_grade)
           WHERE rownum <= i_shownum;
      ELSE
        --只有分支机构和其他
        OPEN o_list FOR
          SELECT stru_id VALUE, stru_sname text
            FROM (SELECT b.stru_id, b.stru_sname
                    FROM dams_branch_relation r, dams_branch b
                   WHERE r.bnch_id = b.stru_id
                     AND r.up_bnch_id = v_stru
                     AND b.stru_sname LIKE '%' || i_stru_name || '%'
                     AND b.stru_sign IN ('1', '4', '8', '18')
                   ORDER BY b.man_grade)
           WHERE rownum <= i_shownum;
      END IF;
    ELSIF (i_type = '3') THEN
      v_stru := pckg_dams_util.func_get_lv1_branch(i_branch_id);
      OPEN o_list FOR
        SELECT p.stru_id VALUE, p.stru_sname text
          FROM (SELECT b.stru_sname, b.stru_id, rownum rn
                  FROM dams_branch_relation r, dams_branch b
                 WHERE r.bnch_id = b.stru_id
                   AND r.up_bnch_id = v_stru
                   AND b.stru_sname LIKE '%' || i_stru_name || '%'
                 ORDER BY b.man_grade) p
         WHERE rn <= i_shownum;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常: ' || v_param || '，error code：' || SQLERRM(SQLCODE));
      --需要主动关闭游标
      IF o_list%ISOPEN THEN
        CLOSE o_list;
      END IF;
      o_ret_code := '-1';
      RETURN;
  END pckg_get_stru_by_name;
  
END pckg_dams_branch;
//


DELIMITER ;//


DELIMITER //;
create or replace package aaa_coverage_tool is

 procedure start_coverage(pck_name varchar2, id out integer);
 procedure add_line(cvr_id    integer,
                     stm_no    integer,
                     start_    integer,
                     end_      integer,
                     txt_start varchar2,
                     txt_end   varchar2);

  procedure end_coverage(id integer);

  procedure abort_coverage(id integer);
  procedure end_coverage(pck_name varchar2);

  procedure clean_up;

  type bool_tab is table of boolean;
  function get_statement_tab(id integer) return bool_tab;
end ;
//


create or replace package body aaa_coverage_tool is

  procedure fetch_source(object_name varchar2,object_type varchar2,source out clob) is
    tmpsrc clob;
  begin
   --DBMS_LOB.CREATETEMPORARY (tmpsrc,true,DBMS_LOB.CALL);
    for r in (select *
                from user_source s
               where s.name = object_name
                 and s.TYPE = object_type
               order by s.line) loop
       DBMS_LOB.WRITEAPPEND(tmpsrc ,length(r.text), r.text);
    end loop;
    source := tmpsrc;
  end;

  procedure start_coverage(pck_name varchar2, id out integer) is
    body_source   clob;
    spec_source   clob;
    source2  clob;
    t         varchar2(200);
    iscovered integer;
    id_temp integer;
  begin
    select object_type
      into t
      from user_objects u
     where u.object_type = 'PACKAGE BODY'
       and u.object_name = pck_name;
    select count(*)
      into iscovered
      from aaa_coverage c
     where c.package_name = start_coverage.pck_name
       and c.is_covered = 1;
    if iscovered > 0 then
      raise_application_error(-20001,
                              'package ' || pck_name || ' is still covered');
    end if;

    delete from aaa_coverage_statements s
     where s.cvr_id in
           (select id from aaa_coverage c where c.package_name = pck_name);
    delete from aaa_coverage c where c.package_name = pck_name ;
    select aaa_coverage_seq.nextval into id_temp from dual;
    start_coverage.id := id_temp;
    insert into aaa_coverage
      (id, package_name, start_date, ORIGINAL_body_SOURCE,ORIGINAL_spec_SOURCE,is_covered)
    values
      (id_temp,pck_name, sysdate, EMPTY_cLOB(),empty_clob(),1)
    returning ORIGINAL_body_SOURCE,original_spec_source into body_source,spec_source;
   
    fetch_source(pck_name,'PACKAGE BODY',source2);
    dbms_lob.append(body_source,source2);
    fetch_source(pck_name,'PACKAGE',source2);
    dbms_lob.append(spec_source,source2);
  end;

  procedure end_coverage(pck_name varchar2) is
    size_  integer;
    cov_id integer;
  begin
    select count(*), max(c.id)
      into size_, cov_id
      from aaa_coverage c
     where c.package_name = end_coverage.pck_name
       and c.end_date is null;
    if size_ = 0 then
      raise_application_error(-20001, 'no coverage open for package: '|| pck_name);
    elsif size_ > 1 then
      raise_application_error(-20001,
                              'more than one coverage open for ' ||
                              pck_name);
    else
      end_coverage(cov_id);
    end if;
  end;

  procedure end_coverage(id integer) is
    r          aaa_coverage%rowtype;
    sql_cursor integer;
  begin
    select * into r from aaa_coverage where id = end_coverage.id;
    update aaa_coverage c
       set c.end_date = sysdate,
           c.is_covered = 0
     where c.id = end_coverage.id;
  end;

  procedure abort_coverage(id integer) is
  begin
    update aaa_coverage c
       set c.end_date = null,
           c.start_date = null,
           c.is_covered = 0
     where c.id = abort_coverage.id;
  end;

  procedure add_line(cvr_id    integer,
                     stm_no    integer,
                     start_    integer,
                     end_      integer,
                     txt_start varchar2,
                     txt_end   varchar2) is
  begin
    insert into aaa_coverage_statements
      (cvr_id, stm_no, line_no, start_, END_, txt_start, txt_end, hit)
    values
      (cvr_id,
       add_line.stm_no,
       -1,
       add_line.start_,
       add_line.end_,
       add_line.txt_start,
       add_line.txt_end,
       0);
  end;
 

  procedure clean_up is
  begin
    delete from aaa_coverage_statements;
    delete from aaa_coverage;
  end;

  function get_statement_tab(id integer) return bool_tab is
    res bool_tab := bool_tab();
    max_stm_no integer;
  begin
    select max(stm_no) into max_stm_no from aaa_coverage_statements s where s.cvr_id = get_statement_tab.id;
    if nvl(max_stm_no,0)=0 then
      return res;
    end if;
    res.extend(max_stm_no);
    for r in (select stm_no from aaa_coverage_statements s where s.cvr_id = get_statement_tab.id and hit=1) loop
      res(r.stm_no):=true;
    end loop;
    return res;
  end;
end;
//

DELIMITER ;//



DELIMITER //;
CREATE OR REPLACE PACKAGE pckg_dams_jbpm IS
  FUNCTION func_jbpm_createtime(i_businessid IN VARCHAR2) --流程定义ID
   RETURN TIMESTAMP;
   
  FUNCTION func_parent_stru(i_struid IN VARCHAR2,
                            i_num    IN NUMBER) --流程定义ID
   RETURN VARCHAR2; 
   
   FUNCTION func_is_ancestor_stru(i_anc_struid IN VARCHAR2,
                                 i_struid     IN VARCHAR2) RETURN INTEGER;
   
   PROCEDURE proc_jbpm_info(i_cfgid         IN VARCHAR2, --配置ID
                           i_pdid          IN VARCHAR2, --流程定义ID
                           i_task_name     IN VARCHAR2, --当前活动名称
                           i_disp_seq      IN NUMBER, --展现序号
                           o_retcode       OUT VARCHAR2, --返回值
                           o_activity_id   OUT VARCHAR2, --活动定义ID
                           o_activity_type OUT VARCHAR2, --活动类型  1-读图 2-走配置
                           o_sys_code      OUT VARCHAR2, --系统编码
                           o_show_type     OUT VARCHAR2, --展现类型
                           o_show_action   OUT VARCHAR2, --初始动作
                           o_show_task     OUT VARCHAR2, --初始任务
                           o_show_dealer   OUT VARCHAR2, --初始处理人
                           o_show_opinion  OUT VARCHAR2, --初始意见
                           o_action_json   OUT VARCHAR2, --动作展现时需要的json
                           o_mail_type     OUT VARCHAR2);
   
    PROCEDURE proc_jbpm_getprocessid(i_struid     IN VARCHAR2,
                                   i_processkey IN VARCHAR2,
                                   i_businessid IN VARCHAR2,
                                   o_retcode    OUT VARCHAR2,
                                   o_pdid       OUT VARCHAR2,
                                   o_formid     OUT VARCHAR2,
                                   o_cfgid      OUT VARCHAR2);
   
   PROCEDURE proc_jbpm_accredit_info(i_mode       IN VARCHAR2, --模式  1.首次申请阶段  2. 审批阶段
                                    i_taskid     IN VARCHAR2, --任务ID
                                    i_userid     IN VARCHAR2, --申请人ID
                                    i_roleid     IN VARCHAR2, --角色ID
                                    i_struid     IN VARCHAR2, --机构ID
                                    i_syscode    IN VARCHAR2, --系统编码
                                    o_retcode    OUT VARCHAR2, --返回值
                                    o_roleid     OUT VARCHAR2, --角色ID
                                    o_struid     OUT VARCHAR2, --机构ID
                                    o_authstate  OUT VARCHAR2, --转授权信息
                                    o_ssicidtran OUT VARCHAR2 --原处理人
                                    );
    PROCEDURE proc_jbpm_opentask(i_ssicid IN VARCHAR2, --用户ID
                               i_taskid IN VARCHAR2, --任务ID
                               i_type   IN VARCHAR2, --0，只读，1，编辑
                               i_cfgid  IN VARCHAR2,
                               --i_lang        IN VARCHAR2, --语言
                               o_flag      OUT VARCHAR2, --存储过程返回标志
                               o_form_url  OUT VARCHAR2, --返回页面
                               o_jbpm_task OUT SYS_REFCURSOR --任务信息
                               );
   
   --PROCEDURE proc_jbpm_tasktraces(i_language   IN VARCHAR2, --语言
   --                              i_formid     IN VARCHAR2, --表单ID
   --                              i_businessid IN VARCHAR2, --业务主键
   --                              i_tracetype  IN VARCHAR2, -- 跟踪类型 1-嵌入式 2弹出式跟踪信息
   --                              o_flag       OUT VARCHAR2, --存储过程返回标志
   --                              o_totalnum   OUT VARCHAR2, --最终获取数
   --                              o_jbpm_trace OUT SYS_REFCURSOR --记录列表
   --                              );
   
   PROCEDURE proc_jbpm_traces_curinfo(i_businessid      IN VARCHAR2, --业务主键
                                     o_flag            OUT VARCHAR2, --存储过程返回标志
                                     o_currenttaskname OUT VARCHAR2, --当前任务名
                                     o_currentusername OUT VARCHAR2 --当前处理人
                                     );
   
   PROCEDURE proc_jbpm_get_retrieve_taskid(i_businessid   IN VARCHAR2,
                                          i_userid       IN VARCHAR2,
                                          o_retcode      OUT VARCHAR2,
                                          o_isflag       OUT VARCHAR2,
                                          o_retaskid     OUT VARCHAR2,
                                          o_retaskname   OUT VARCHAR2,
                                          o_reactivityid OUT VARCHAR2,
                                          o_reroleid     OUT VARCHAR2,
                                          o_restruid     OUT VARCHAR2,
                                          o_curtaskid    OUT VARCHAR2,
                                          o_formid       OUT VARCHAR2,
                                          o_syscode      OUT VARCHAR2);
   
   PROCEDURE proc_jbpm_taskassignees(i_ssicid              IN VARCHAR2, --用户id
                                    i_cfgid               IN VARCHAR2, --配置ID
                                    i_pdid                IN VARCHAR2, --流程定义ID
                                    i_execute_id          IN VARCHAR2, --流程实例ID
                                    i_activity_id         IN VARCHAR2, --下一活动id
                                    i_task_name           IN VARCHAR2, --下一活动名称
                                    i_transition_name     IN VARCHAR2, --流入的动作名称
                                    i_role_id             IN VARCHAR2, --处理人权限
                                    i_stru_id             IN VARCHAR2, --处理人权限范围
                                    i_sys_code            IN VARCHAR2, --系统编号
                                    i_language            IN VARCHAR2, --语种
                                    i_jbpm_assignee_param IN VARCHAR2, --下一处理人扩展参数
                                    o_flag                OUT VARCHAR2, --存储过程返回标志
                                    o_cacu_type           OUT VARCHAR2, --1.单选  2.会签  3.竞争
                                    o_show_type           OUT VARCHAR2, --展现类型 1.到人 2.到机构 3.到角色
                                    o_multiuser_flag      OUT VARCHAR2, --M-多人，S-单人
                                    o_mail_type           OUT VARCHAR2, --邮件类型
                                    o_ext_interface       OUT VARCHAR2, --使用的java处理类
                                    o_jbpm_task_assignee  OUT SYS_REFCURSOR --任务参与者
                                    );
   
   PROCEDURE proc_jbpm_role(i_cfgid     IN VARCHAR2, --配置ID
                           i_pdid      IN VARCHAR2, --流程定义ID
                           i_proincid  IN VARCHAR2, --流程实例ID
                           i_task_id   IN VARCHAR2, --任务ID
                           i_task_name IN VARCHAR2, --当前活动名称
                           i_ssic_id   IN VARCHAR2, --当前处理人
                           i_stru_id   IN VARCHAR2, --传入申请机构id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_role_id   OUT VARCHAR2, --角色id
                           o_stru_id   OUT VARCHAR2 --机构id
                           );
   
   PROCEDURE proc_jbpm_prev_audit_info(i_task_id        IN VARCHAR2, --流程定义ID
                                      o_flag           OUT VARCHAR2, --存储过程返回标志
                                      o_prev_auditinfo OUT SYS_REFCURSOR --上一步处理人信息
                                      );
   
   PROCEDURE proc_jbpm_activity_alias(i_pdid       IN VARCHAR2, --流程定义ID
                                     i_cfgid      IN VARCHAR2, --配置项ID
                                     i_transition IN VARCHAR2, --流入路径名
                                     i_originname IN VARCHAR2, --图上活动名称
                                     o_flag       OUT VARCHAR2, --存储过程返回标志
                                     o_aliasname  OUT VARCHAR2 --别名
                                     );
   
   PROCEDURE proc_jbpm_activity_origin(i_pdid       IN VARCHAR2, --流程定义ID
                                      i_cfgid      IN VARCHAR2, --配置项ID
                                      i_transition IN VARCHAR2, --流入路径名
                                      i_aliasname  IN VARCHAR2, --别名
                                      o_flag       OUT VARCHAR2, --存储过程返回标志
                                      o_originname OUT VARCHAR2 --图上活动名称
                                      );

   --打开申请，根据business_id查询基本信息
   PROCEDURE proc_taskqry_open(i_businessid IN VARCHAR2,
                              i_user_id    IN VARCHAR2,
                              i_type       IN VARCHAR2, --1只查询自身参与过的文,0不限制
                              i_sys_code   IN VARCHAR2,
                              o_flag       OUT VARCHAR2,
                              o_state      OUT VARCHAR2, --todo代办done已办over完成
                              o_task_id    OUT VARCHAR2);

   --取回（已处理过的环节处理人都可取回）
   PROCEDURE proc_get_retrieve_info(i_businessid    IN VARCHAR2,
                                   i_userid        IN VARCHAR2,
                                   o_retcode       OUT VARCHAR2,
                                   o_msg           OUT VARCHAR2,
                                   o_isflag        OUT VARCHAR2,
                                   o_retrieve_info OUT SYS_REFCURSOR);
   
END pckg_dams_jbpm;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_jbpm IS
    -- code for logging
	"$hits" aaa_coverage_tool.bool_tab; 
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	  begin
		 insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(45);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(45);
		dbms_output.put_line('There is no such letter get_statement_time_tab in aaa_coverage_tool');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(45,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(45,x); "$times"(x):=true; 
	  end if;
	end;
	
  FUNCTION func_jbpm_createtime(i_businessid IN VARCHAR2) --流程定义ID
    RETURN TIMESTAMP IS
      v_tm TIMESTAMP;
  BEGIN
		BEGIN
		  SELECT t.start_time
			INTO v_tm
			FROM dams_wf_process t
		   WHERE t.business_id = i_businessid;
		EXCEPTION
		  WHEN no_data_found THEN
			SELECT MAX(t.start_time)
			  INTO v_tm
			  FROM dams_wf_hist_process t
			 WHERE t.business_id = i_businessid;
		END;
		RETURN v_tm;
  END func_jbpm_createtime;
	
  FUNCTION func_parent_stru(i_struid IN VARCHAR2,
                            i_num    IN NUMBER) RETURN VARCHAR2 IS
    v_stru_id dams_wf_task.stru_id%TYPE;
  BEGIN
    IF i_num IS NULL OR i_num = 0 THEN
      RETURN i_struid;
    ELSE
      SELECT MAX(b.sup_stru)
        INTO v_stru_id
        FROM dams_branch b
       WHERE b.stru_id = i_struid;
    
      IF v_stru_id IS NULL THEN
        RETURN i_struid;
      ELSE
        RETURN func_parent_stru(v_stru_id, i_num - 1);
      END IF;
    END IF;
  END func_parent_stru;
  
  FUNCTION func_is_ancestor_stru(i_anc_struid IN VARCHAR2,
                                 i_struid     IN VARCHAR2) RETURN INTEGER IS
    v_stru_id dams_wf_task.stru_id%TYPE;
  BEGIN
    IF i_struid IS NULL THEN
      RETURN 0;
    ELSIF i_anc_struid = i_struid THEN
      RETURN 1;
    ELSE
      SELECT MAX(b.sup_stru)
        INTO v_stru_id
        FROM dams_branch b
       WHERE b.stru_id = i_struid;
      RETURN func_is_ancestor_stru(i_anc_struid, v_stru_id);
    END IF;
  END func_is_ancestor_stru;
  
  PROCEDURE proc_jbpm_info(i_cfgid         IN VARCHAR2, --配置ID
                           i_pdid          IN VARCHAR2, --流程定义ID
                           i_task_name     IN VARCHAR2, --当前活动名称
                           i_disp_seq      IN NUMBER, --展现序号
                           o_retcode       OUT VARCHAR2, --返回值
                           o_activity_id   OUT VARCHAR2, --活动定义ID
                           o_activity_type OUT VARCHAR2, --活动类型  1-读图 2-走配置
                           o_sys_code      OUT VARCHAR2, --系统编码
                           o_show_type     OUT VARCHAR2, --展现类型
                           o_show_action   OUT VARCHAR2, --初始动作
                           o_show_task     OUT VARCHAR2, --初始任务
                           o_show_dealer   OUT VARCHAR2, --初始处理人
                           o_show_opinion  OUT VARCHAR2, --初始意见
                           o_action_json   OUT VARCHAR2, --动作展现时需要的json
                           o_mail_type     OUT VARCHAR2) IS
    temp        VARCHAR2(255);
    v_param     db_log.info%TYPE := i_cfgid || '|' || i_pdid || '|' || i_task_name || '|' || i_disp_seq;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_info';
    v_step      db_log.step_no%TYPE;
    v_sql1      VARCHAR2(1000);
    v_sql2      VARCHAR2(1000);
    v_sql3      VARCHAR2(1000);
    v_sql       VARCHAR2(1000);
  BEGIN
    o_retcode := '0';
    pack_log.info(v_proc_name, pack_log.start_step,  pack_log.start_msg || v_param);
    
    SELECT c.id,
           c.activity_type,
           p.sys_code,
           d.show_type,
           d.show_action,
           d.show_task,
           d.show_dealer,
           d.show_opinion,
           d.action_btn,
           c.mail_type
      INTO o_activity_id,
           o_activity_type,
           o_sys_code,
           o_show_type,
           o_show_action,
           o_show_task,
           o_show_dealer,
           o_show_opinion,
           o_action_json,
           o_mail_type
      FROM dams_wf_activity_conf    c,
           dams_wf_activity_display d,
           dams_wf_process_conf     p
     WHERE c.id = d.id
       AND c.pdid = d.pdid
       AND p.pdid = d.pdid
       AND c.activity_name = i_task_name
       AND c.pdid = i_pdid
       AND c.cfgid = i_cfgid
       AND d.disp_seq = nvl(i_disp_seq, 1);
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_jbpm_info;
  
  PROCEDURE proc_jbpm_getprocessid(i_struid     IN VARCHAR2,
                                   i_processkey IN VARCHAR2,
                                   i_businessid IN VARCHAR2,
                                   o_retcode    OUT VARCHAR2,
                                   o_pdid       OUT VARCHAR2,
                                   o_formid     OUT VARCHAR2,
                                   o_cfgid      OUT VARCHAR2) IS
    temp         VARCHAR2(255);
    v_param      db_log.info%TYPE := i_struid || '|' || i_processkey || '|' ||
                                     i_businessid;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_getprocessid';
    v_step       db_log.step_no%TYPE;
    v_processkey VARCHAR2(255);
    v_count      INTEGER;
  BEGIN
    o_retcode := '0';
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  
    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT p.process_id
              FROM dams_wf_process p, dams_wf_process_form f
             WHERE p.process_id = f.pdid
               AND p.business_id = i_businessid
            UNION ALL
            SELECT p.process_id
              FROM dams_wf_hist_process p, dams_wf_process_form f
             WHERE p.process_id = f.pdid
               AND p.business_id = i_businessid);
  
    IF v_count = 0 THEN
      --申请时获得pdid(总分行有别)   
      SELECT t1.pdkey_mirror, t1.cfgid
        INTO v_processkey, o_cfgid
        FROM (SELECT t.pdkey_mirror,
                     t.cfgid,
                     b.up_level,
                     MIN(b.up_level) over(PARTITION BY 1) minlv
                FROM dams_wf_stru_range_config t, dams_branch_relation b
               WHERE t.stru_range = b.up_bnch_id
                 AND b.bnch_id = i_struid
                 AND t.pdkey = i_processkey) t1
       WHERE t1.up_level = t1.minlv;
    
      SELECT MAX(d.stringval_)
        INTO o_pdid
        FROM jbpm4_deployprop d
       WHERE key_ = 'pdid'
         AND d.stringval_ LIKE (v_processkey || '-%');
    
      SELECT f.form_id
        INTO o_formid
        FROM dams_wf_process_form f
       WHERE f.pdid = o_pdid;
    
    ELSE
      --审批时，从申请表中取
      SELECT COUNT(1)
        INTO v_count
        FROM viw_dams_wf_all_process p, dams_wf_process_form f
       WHERE p.process_id = f.pdid
         AND p.business_id = i_businessid;
      IF v_count > 1 THEN
        -- 如果一流程多种formid
        SELECT p.process_id, p.cfg_id
          INTO o_pdid, o_cfgid
          FROM viw_dams_wf_all_process p
         WHERE p.business_id = i_businessid;
        SELECT t.form_id
          INTO o_formid
          FROM viw_dams_wf_all_task t
         WHERE t.business_id = i_businessid
           AND rownum = 1;
      ELSE
        BEGIN
          SELECT p.process_id, p.cfg_id, f.form_id
            INTO o_pdid, o_cfgid, o_formid
            FROM dams_wf_process p, dams_wf_process_form f
           WHERE p.process_id = f.pdid
             AND p.business_id = i_businessid;
        EXCEPTION
          WHEN no_data_found THEN
            SELECT p.process_id, p.cfg_id, f.form_id
              INTO o_pdid, o_cfgid, o_formid
              FROM dams_wf_hist_process p, dams_wf_process_form f
             WHERE p.process_id = f.pdid
               AND p.business_id = i_businessid;
        END;
      END IF;
    
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_jbpm_getprocessid;
  
  PROCEDURE proc_jbpm_accredit_info(i_mode       IN VARCHAR2, --模式  1.首次申请阶段  2. 审批阶段
                                    i_taskid     IN VARCHAR2, --任务ID
                                    i_userid     IN VARCHAR2, --申请人ID
                                    i_roleid     IN VARCHAR2, --角色ID
                                    i_struid     IN VARCHAR2, --机构ID
                                    i_syscode    IN VARCHAR2, --系统编码
                                    o_retcode    OUT VARCHAR2, --返回值
                                    o_roleid     OUT VARCHAR2, --角色ID
                                    o_struid     OUT VARCHAR2, --机构ID
                                    o_authstate  OUT VARCHAR2, --转授权信息
                                    o_ssicidtran OUT VARCHAR2 --原处理人
                                    ) IS
    v_param      db_log.info%TYPE := i_mode || '|' || i_taskid || '|' ||
                                     i_userid || '|' || i_roleid || '|' ||
                                     i_struid || '|' || i_syscode;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_accredit_info';
    v_step       db_log.step_no%TYPE;
    v_sql        VARCHAR2(1000);
    v_auth_state dams_user_role_rel.auth_state%TYPE;
  BEGIN
    o_retcode := '0';
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    --首次申请
    IF i_mode = '1' THEN
      o_roleid := i_roleid;
      o_struid := i_struid;
      BEGIN
        SELECT ur.auth_state
          INTO v_auth_state
          FROM dams_user_role_rel ur
         WHERE ur.ssic_id = i_userid
           AND ur.role_id = i_roleid
           AND ur.stru_id = i_struid
           AND ur.sys_code = i_syscode;
      
        IF v_auth_state = '0' THEN
          o_authstate  := '0';
          o_ssicidtran := '';
        ELSE
          SELECT m.source_user_id
            INTO o_ssicidtran
            FROM dams_accredit_main m
           WHERE m.sys_code = i_syscode
             AND m.current_user_id = i_userid
             AND m.role_id = i_roleid
             AND m.bnch_id = i_struid
             AND m.state = '1';
        
          --存在 ,则此用户为受权人
          o_authstate := '1';
        END IF;
      
      EXCEPTION
        WHEN no_data_found THEN
          --不存在，则此用户未做授权
          o_authstate  := '0';
          o_ssicidtran := '';
      END;
    
    ELSE
      --审批阶段
      SELECT t.role_id, t.stru_id, t.auth_state, t.ssic_id_tran
        INTO o_roleid, o_struid, o_authstate, o_ssicidtran
        FROM dams_wf_participation t, dams_wf_task l
       WHERE t.business_id = l.business_id
         AND t.pre_task_id = l.prev_task_id
         AND l.task_id = i_taskid
         AND t.ssic_id = i_userid;
    
    END IF;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode    := '1';
      o_roleid     := '';
      o_struid     := '';
      o_authstate  := '0';
      o_ssicidtran := '';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_jbpm_accredit_info;
  
  PROCEDURE proc_jbpm_opentask(i_ssicid IN VARCHAR2, --用户ID
                               i_taskid IN VARCHAR2, --任务ID
                               i_type   IN VARCHAR2, --0，只读，1，编辑
                               i_cfgid  IN VARCHAR2,
                               --i_lang        IN VARCHAR2, --语言
                               o_flag      OUT VARCHAR2, --存储过程返回标志
                               o_form_url  OUT VARCHAR2, --返回页面
                               o_jbpm_task OUT SYS_REFCURSOR --任务信息
                               ) IS
  
    v_form_id            VARCHAR2(255);
    v_task_name          VARCHAR2(255);
    v_process_def_id     VARCHAR2(255);
    v_activity_config_id VARCHAR2(255);
    v_count              INTEGER;
    v_state              VARCHAR2(20); -- T-待办或已办 H-办结
    v_param              db_log.info%TYPE := i_ssicid || '|' || i_taskid || '|' ||
                                             i_type || '|' || i_cfgid;
    v_proc_name          db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_JBPM_OPENTASK';
    v_step               db_log.step_no%TYPE;
    v_dealssic_count     INTEGER := 0; --判断是否已填充处理人信息0否1是
    v_cacu_type          dams_wf_activity_conf.cacu_type%TYPE;
  BEGIN
    o_flag  := '0';
    v_count := 0;
  
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    SELECT COUNT(1)
      INTO v_count
      FROM dams_wf_task
     WHERE task_id = to_number(i_taskid);
  
    --办结
    IF (v_count = 0) THEN
      OPEN o_jbpm_task FOR
        SELECT t.task_id,
               t.task_name,
               t.ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.ssic_id) user_name,
               t.stru_id,
               t.role_id,
               t.prev_task_id,
               t.prev_ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.prev_ssic_id) prev_person_name,
               t.prev_stru_id,
               t.prev_role_id,
               p.creator_id starter_ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = p.creator_id) starter_name,
               p.procinst_id,
               p.process_id,
               p.process_name,
               t.form_id,
               p.business_type,
               p.business_id,
               p.title,
               t.create_time,
               t.take_time,
               t.end_time,
               p.start_time proc_start_time,
               p.complete_time proc_completed_time,
               t.task_state,
               t.outgoing_name,
               t.opinion,
               t.is_first,
               '' no_assign_flag,
               t.activity_id,
               (SELECT nvl(MAX(c.cacu_type), '1')
                  FROM dams_wf_activity_conf c
                 WHERE c.id = t.activity_id
                   AND c.cfgid = p.cfg_id) cacu_type
        
          FROM dams_wf_hist_task t, dams_wf_hist_process p
         WHERE t.procinst_id = p.procinst_id
           AND t.task_id = to_number(i_taskid);
    
      SELECT t.form_id, p.process_id, t.task_name, t.task_state
        INTO v_form_id, v_process_def_id, v_task_name, v_state
        FROM dams_wf_hist_task t, dams_wf_hist_process p
       WHERE t.procinst_id = p.procinst_id
         AND t.task_id = to_number(i_taskid);
    
      SELECT MAX(id), MAX(cacu_type)
        INTO v_activity_config_id, v_cacu_type
        FROM dams_wf_activity_conf
       WHERE cfgid = nvl(i_cfgid, cfgid)
         AND pdid = v_process_def_id
         AND activity_name = v_task_name;
    
      SELECT MAX(read_url)
        INTO o_form_url
        FROM dams_wf_activity_form
       WHERE activity_id = v_activity_config_id
         AND form_id = v_form_id;
    
      IF (o_form_url IS NULL OR o_form_url = '') THEN
        SELECT read_url
          INTO o_form_url
          FROM dams_form
         WHERE form_id = v_form_id;
      END IF;
      RETURN;
    END IF;
  
    --待办或已办
    IF (v_count > 0) THEN
    
      SELECT t.form_id,
             p.process_id,
             t.task_name,
             t.task_state,
             nvl2(t.ssic_id, 1, 0),
             c.cacu_type
        INTO v_form_id,
             v_process_def_id,
             v_task_name,
             v_state,
             v_dealssic_count,
             v_cacu_type
        FROM dams_wf_task t, dams_wf_process p, dams_wf_activity_conf c
       WHERE t.procinst_id = p.procinst_id
         AND t.task_id = to_number(i_taskid)
         AND c.cfgid = nvl(i_cfgid, cfgid)
         AND c.pdid = p.process_id
         AND c.activity_name = t.task_name;
    
      --已办或废止查看
      IF (v_state = 'completed' OR v_state = 'drop' OR i_type = '0') THEN
      
        SELECT MAX(id)
          INTO v_activity_config_id
          FROM dams_wf_activity_conf
         WHERE cfgid = nvl(i_cfgid, cfgid)
           AND pdid = v_process_def_id
           AND activity_name = v_task_name;
      
        SELECT MAX(read_url)
          INTO o_form_url
          FROM dams_wf_activity_form
         WHERE activity_id = v_activity_config_id
           AND form_id = v_form_id;
      
        IF (o_form_url IS NULL OR o_form_url = '') THEN
          SELECT read_url
            INTO o_form_url
            FROM dams_form
           WHERE form_id = v_form_id;
        END IF;
      
        --待办
      ELSIF (v_state = 'open' AND i_type = '1') THEN
        --获取JBPM定义的url
        SELECT MAX(form_)
          INTO o_form_url
          FROM jbpm4_task
         WHERE dbid_ = to_number(i_taskid);
      
        IF (o_form_url IS NULL OR o_form_url = '') THEN
          SELECT MAX(id)
            INTO v_activity_config_id
            FROM dams_wf_activity_conf
           WHERE cfgid = nvl(i_cfgid, cfgid)
             AND pdid = v_process_def_id
             AND activity_name = v_task_name;
        
          SELECT MAX(form_url)
            INTO o_form_url
            FROM dams_wf_activity_form
           WHERE activity_id = v_activity_config_id
             AND form_id = v_form_id;
        
          IF (o_form_url IS NULL OR o_form_url = '') THEN
            SELECT form_url
              INTO o_form_url
              FROM dams_form
             WHERE form_id = v_form_id;
          END IF;
        END IF;
      
        -- 代办打开 填充处理人信息
        IF v_cacu_type = '4' THEN
          -- cacu type4非抢占，不做填充
          NULL;
        ELSIF v_dealssic_count = 0 THEN
          --填充处理人信息
          UPDATE dams_wf_task c
             SET (c.ssic_id, c.stru_id, c.role_id) =
                 (SELECT p.ssic_id, p.stru_id, p.role_id
                    FROM dams_wf_participation p
                   WHERE p.pre_task_id = c.prev_task_id
                     AND p.ssic_id = i_ssicid
                     AND rownum = 1)
           WHERE c.task_id = to_number(i_taskid)
             AND c.ssic_id IS NULL;
        END IF;
      
        --设置查看时间
        UPDATE dams_wf_task
           SET take_time = SYSDATE
         WHERE task_id = to_number(i_taskid)
           AND take_time IS NULL;
      END IF;
    
      OPEN o_jbpm_task FOR
        SELECT t.task_id,
               t.task_name,
               decode(v_cacu_type, '4', i_ssicid, t.ssic_id) ssic_id,
               decode(v_cacu_type, '4',
                      (SELECT u.name FROM dams_user u WHERE u.ssic_id = i_ssicid),
                      (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.ssic_id)) user_name,
               decode('4', '4',
                      (SELECT t2.stru_id
                          FROM dams_wf_task t1, dams_wf_participation t2
                         WHERE t1.task_id = to_number(i_taskid)
                           AND t1.prev_task_id = t2.pre_task_id
                           AND t2.ssic_id = i_ssicid
                           AND rownum = 1), t.stru_id) stru_id,
               decode('4', '4',
                      (SELECT t2.role_id
                          FROM dams_wf_task t1, dams_wf_participation t2
                         WHERE t1.task_id = to_number(i_taskid)
                           AND t1.prev_task_id = t2.pre_task_id
                           AND t2.ssic_id = i_ssicid
                           AND rownum = 1), t.role_id) role_id,
               t.prev_task_id,
               t.prev_ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.prev_ssic_id) prev_person_name,
               t.prev_stru_id,
               t.prev_role_id,
               p.creator_id starter_ssic_id,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = p.creator_id) starter_name,
               p.procinst_id,
               p.process_id,
               p.process_name,
               t.form_id,
               p.business_type,
               p.business_id,
               p.title,
               t.create_time,
               t.take_time,
               t.end_time,
               p.start_time proc_start_time,
               p.complete_time proc_completed_time,
               t.task_state,
               t.outgoing_name,
               t.opinion,
               t.is_first,
               decode(v_cacu_type, '4', '1', '') no_assign_flag,
               t.activity_id,
               (SELECT nvl(MAX(c.cacu_type), '1')
                  FROM dams_wf_activity_conf c
                 WHERE c.id = t.activity_id
                   AND c.cfgid = p.cfg_id) cacu_type
          FROM dams_wf_task t, dams_wf_process p
         WHERE t.procinst_id = p.procinst_id
           AND t.task_id = to_number(i_taskid);
    
    END IF;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  
  EXCEPTION
    WHEN OTHERS THEN
      IF o_jbpm_task%ISOPEN THEN
        CLOSE o_jbpm_task;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_flag := '-1';
      RETURN;
  END proc_jbpm_opentask;
  
  --PROCEDURE proc_jbpm_tasktraces(i_language   IN VARCHAR2, --语言
  --                               i_formid     IN VARCHAR2, --表单ID
  --                               i_businessid IN VARCHAR2, --业务主键
  --                               i_tracetype  IN VARCHAR2, -- 跟踪类型 1-嵌入式 2弹出式跟踪信息
  --                               o_flag       OUT VARCHAR2, --存储过程返回标志
  --                               o_totalnum   OUT VARCHAR2, --最终获取数
  --                               o_jbpm_trace OUT SYS_REFCURSOR --记录列表
  --                               ) IS
  --  v_param     db_log.info%TYPE := i_language || '|' || i_formid || '|' ||
  --                                  i_businessid;
  --  v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_tasktraces';
  --  v_step      db_log.step_no%TYPE;
  --BEGIN
  --  o_flag := '0';
  --
  --  pack_log.info(v_proc_name, pack_log.start_step,
  --                pack_log.start_msg || v_param);    
  --  WITH buss_table AS
  --   (SELECT x.business_id, x.pre_task_id
  --      FROM dams_wf_sub_process_rel x
  --     START WITH x.source_business_id = i_businessid
  --    CONNECT BY x.source_business_id = PRIOR x.business_id
  --    UNION ALL
  --    SELECT d.business_id, nvl(r.pre_task_id, 0)
  --      FROM (SELECT i_businessid business_id FROM dual) d,
  --           dams_wf_sub_process_rel r
  --     WHERE d.business_id = r.business_id(+))
  --  SELECT COUNT(1)
  --    INTO o_totalnum
  --    FROM (SELECT t.task_id,
  --                 task_name,
  --                 nvl(t.ssic_id, subt.ssic_id) ssic_id,
  --                 nvl2(t.ssic_id,
  --                      nvl2(t.stru_sname,
  --                            t.user_name || '[' || t.stru_sname || ']', t.user_name),
  --                      subt.user_name) user_name,
  --                 nvl(t.stru_id, subt.stru_id) stru_id,
  --                 nvl(t.stru_sname, subt.branch_name) branch_name,
  --                 t.prev_task_id,
  --                 t.prev_ssic_id,
  --                 t.prev_user_name,
  --                 t.creator_id starter_ssic_id,
  --                 t.prev_stru_id,
  --                 t.procinst_id,
  --                 t.process_id,
  --                 t.process_name,
  --                 t.form_id,
  --                 t.business_type,
  --                 t.business_id,
  --                 t.title,
  --                 t.create_time,
  --                 t.take_time,
  --                 t.end_time,
  --                 t.complete_time,
  --                 t.task_state,
  --                 nvl(a.activity_name_alias,
  --                     decode(t.outgoing_name, 'default_transition', t.task_name,
  --                             t.outgoing_name)) outgoing_name,
  --                 t.opinion,
  --                 'a' action_name,
  --                 nvl2(t.ssic_id, t.user_name, subt.user_name) user_name2,
  --                 t.start_point,
  --                 t.f_ssic_id,
  --                 t.is_first
  --            FROM (SELECT tt.ssic_id,
  --                         tt.task_id,
  --                         tt.procinst_id,
  --                         tt.role_id,
  --                         tt.stru_id,
  --                         tt.prev_task_id,
  --                         tt.prev_ssic_id,
  --                         tt.prev_stru_id,
  --                         tt.form_id,
  --                         tt.create_time,
  --                         tt.take_time,
  --                         tt.end_time,
  --                         tt.task_state,
  --                         tt.outgoing_name,
  --                         tt.opinion,
  --                         tt.is_first,
  --                         tt.activity_id,
  --                         p.cfg_id,
  --                         p.process_id,
  --                         p.process_name,
  --                         p.creator_id,
  --                         p.title,
  --                         p.business_id,
  --                         p.business_type,
  --                         p.complete_time,
  --                         (SELECT bb.stru_sname
  --                            FROM dams_branch bb
  --                           WHERE bb.stru_id = tt.stru_id) stru_sname,
  --                         (SELECT uu.name
  --                            FROM dams_user uu
  --                           WHERE uu.ssic_id = tt.ssic_id) user_name,
  --                         (SELECT u.name
  --                            FROM dams_user u
  --                           WHERE u.ssic_id = tt.prev_ssic_id) prev_user_name,
  --                         tt.action_flag action_flag,
  --                         tt.task_name,
  --                         decode(tt.prev_task_id, b.pre_task_id, 'start', '') start_point,
  --                         tt.ssic_id f_ssic_id
  --                    FROM dams_wf_task tt, dams_wf_process p, buss_table b
  --                   WHERE tt.business_id = b.business_id
  --                     AND tt.procinst_id = p.procinst_id
  --                  UNION ALL
  --                  SELECT tt.ssic_id,
  --                         tt.task_id,
  --                         tt.procinst_id,
  --                         tt.role_id,
  --                         tt.stru_id,
  --                         tt.prev_task_id,
  --                         tt.prev_ssic_id,
  --                         tt.prev_stru_id,
  --                         tt.form_id,
  --                         tt.create_time,
  --                         tt.take_time,
  --                         tt.end_time,
  --                         tt.task_state,
  --                         tt.outgoing_name,
  --                         tt.opinion,
  --                         tt.is_first,
  --                         tt.activity_id,
  --                         p.cfg_id,
  --                         p.process_id,
  --                         p.process_name,
  --                         p.creator_id,
  --                         p.title,
  --                         p.business_id,
  --                         p.business_type,
  --                         p.complete_time,
  --                         (SELECT bb.stru_sname
  --                            FROM dams_branch bb
  --                           WHERE bb.stru_id = tt.stru_id) stru_sname,
  --                         (SELECT uu.name
  --                            FROM dams_user uu
  --                           WHERE uu.ssic_id = tt.ssic_id) user_name,
  --                         (SELECT u.name
  --                            FROM dams_user u
  --                           WHERE u.ssic_id = tt.prev_ssic_id) prev_user_name,
  --                         tt.action_flag action_flag,
  --                         tt.task_name,
  --                         decode(tt.prev_task_id, b.pre_task_id, 'start', '') start_point,
  --                         tt.ssic_id f_ssic_id
  --                    FROM dams_wf_hist_task    tt,
  --                         dams_wf_hist_process p,
  --                         buss_table           b
  --                   WHERE tt.business_id = b.business_id
  --                     AND tt.procinst_id = p.procinst_id) t,
  --                 (SELECT t1.ssic_id, --dams_conn(t1.ssic_id) ssic_id,
  --                         nvl2(b1.stru_sname, u1.name || '[' || b1.stru_sname || ']', u1.name) user_name, --dams_conn(nvl2(b1.stru_sname, u1.name || '[' || b1.stru_sname || ']', u1.name)) user_name,
  --                         b1.stru_id, --dams_conn(b1.stru_id) stru_id,
  --                         b1.stru_sname, --dams_conn(b1.stru_sname) branch_name,
  --                         t1.pre_task_id,
  --                         t1.procinst_id
  --                    FROM dams_wf_participation t1,
  --                         dams_user             u1,
  --                         dams_branch           b1,
  --                         buss_table            bs
  --                   WHERE t1.ssic_id = u1.ssic_id
  --                     AND t1.stru_id = b1.stru_id(+)
  --                     AND t1.business_id = bs.business_id
  --                   GROUP BY t1.pre_task_id, t1.procinst_id) subt,
  --                 dams_wf_activity_alias a
  --           WHERE t.prev_task_id = subt.pre_task_id(+)
  --             AND t.procinst_id = subt.procinst_id(+)
  --             AND t.cfg_id = a.cfgid(+)
  --             AND t.outgoing_name = a.incoming_transition(+)
  --             AND t.activity_id = a.activity_id(+)
  --          UNION ALL
  --          SELECT NULL task_id,
  --                 '会办人员处理' task_name,
  --                 au.assist_user_id,
  --                 du.name user_name,
  --                 au.stru_id AS stru_id,
  --                 (SELECT bb.stru_sname
  --                    FROM dams_branch bb
  --                   WHERE bb.stru_id = au.stru_id) stru_sname,
  --                 au.task_id prev_task_id,
  --                 iu.prev_ssic_id prev_ssic_id,
  --                 (SELECT iuu.name
  --                    FROM dams_user iuu
  --                   WHERE iuu.ssic_id = iu.prev_ssic_id) prev_user_name,
  --                 iu.creator_id starter_ssic_id,
  --                 '' prev_stru_id,
  --                 '' procinst_id,
  --                 '' process_id,
  --                 '' process_name,
  --                 '' form_id,
  --                 iu.business_type business_type,
  --                 au.business_id,
  --                 iu.title title,
  --                 iu.create_time,
  --                 iu.take_time,
  --                 iu.end_time,
  --                 NULL complete_time,
  --                 iu.task_state,
  --                 decode(au.status, '1', '会办完成') outgoing_name,
  --                 au.assist_user_comment opinion,
  --                 'a' action_name,
  --                 du.name user_name2,
  --                 '' start_point,
  --                 au.assist_user_id f_ssic_id,
  --                 '0' is_first
  --            FROM dams_assist_user        au,
  --                 dams_user               du,
  --                 viw_dams_inner_all_task iu,
  --                 buss_table              bt
  --           WHERE au.assist_user_id = du.ssic_id(+)
  --             AND au.business_id = bt.business_id
  --             AND decode(au.status, '0', 'open', 'completed') = iu.task_state
  --             AND au.business_id = iu.business_id
  --             AND iu.task_id = to_char(au.task_id)
  --             AND au.assist_user_id = iu.ssic_id) rd
  --   WHERE (i_tracetype <> '1' OR
  --         (rd.is_first <> '1' AND rd.f_ssic_id IS NOT NULL));
  --
  --  OPEN o_jbpm_trace FOR
  --    WITH buss_table AS
  --     (SELECT x.business_id, x.pre_task_id
  --        FROM dams_wf_sub_process_rel x
  --       START WITH x.source_business_id = i_businessid
  --      CONNECT BY x.source_business_id = PRIOR x.business_id
  --      UNION ALL
  --      SELECT d.business_id, nvl(r.pre_task_id, 0)
  --        FROM (SELECT i_businessid business_id FROM dual) d,
  --             dams_wf_sub_process_rel r
  --       WHERE d.business_id = r.business_id(+))
  --    SELECT *
  --      FROM (SELECT rd.task_id,
  --                   rd.task_name,
  --                   rd.ssic_id,
  --                   decode(rd.task_name, '财务受理岗', '财务受理岗', rd.user_name),
  --                   rd.stru_id,
  --                   rd.branch_name,
  --                   rd.prev_task_id,
  --                   rd.prev_ssic_id,
  --                   rd.prev_user_name,
  --                   rd.starter_ssic_id,
  --                   (SELECT u.name
  --                      FROM dams_user u
  --                     WHERE u.ssic_id = rd.starter_ssic_id) starter_name,
  --                   rd.prev_stru_id,
  --                   rd.procinst_id,
  --                   rd.process_id,
  --                   rd.process_name,
  --                   rd.form_id,
  --                   rd.business_type,
  --                   rd.business_id,
  --                   rd.title,
  --                   to_char(rd.create_time, 'yyyy-mm-dd hh24:mi:ss') create_time,
  --                   to_char(rd.take_time, 'yyyy-mm-dd hh24:mi:ss') take_time,
  --                   to_char(rd.end_time, 'yyyy-mm-dd hh24:mi:ss') end_time,
  --                   to_char(rd.complete_time, 'yyyy-mm-dd hh24:mi:ss') complete_time,
  --                   rd.task_state,
  --                   rd.outgoing_name,
  --                   rd.opinion,
  --                   rd.action_name,
  --                   rd.user_name2,
  --                   rd.f_ssic_id,
  --                   rd.is_first,
  --                   rownum rn
  --              FROM (SELECT t.task_id,
  --                           task_name,
  --                           nvl(t.ssic_id, subt.ssic_id) ssic_id,
  --                           nvl2(t.ssic_id, t.user_name, subt.user_name) user_name,
  --                           nvl(t.stru_id, subt.stru_id) stru_id,
  --                           nvl(t.stru_sname, subt.branch_name) branch_name,
  --                           t.prev_task_id,
  --                           t.prev_ssic_id,
  --                           t.prev_user_name,
  --                           t.creator_id starter_ssic_id,
  --                           t.prev_stru_id,
  --                           t.procinst_id,
  --                           t.process_id,
  --                           t.process_name,
  --                           t.form_id,
  --                           t.business_type,
  --                           t.business_id,
  --                           t.title,
  --                           t.create_time,
  --                           t.take_time,
  --                           t.end_time,
  --                           t.complete_time,
  --                           t.task_state,
  --                           nvl(a.activity_name_alias,
  --                               decode(t.outgoing_name, 'default_transition',
  --                                       t.task_name, t.outgoing_name)) outgoing_name,
  --                           t.opinion,
  --                           'a' action_name,
  --                           nvl2(t.ssic_id, t.user_name, subt.user_name) user_name2,
  --                           t.start_point,
  --                           t.f_ssic_id,
  --                           t.is_first
  --                      FROM (SELECT tt.ssic_id,
  --                                   tt.task_id,
  --                                   tt.procinst_id,
  --                                   tt.role_id,
  --                                   tt.stru_id,
  --                                   tt.prev_task_id,
  --                                   tt.prev_ssic_id,
  --                                   tt.prev_stru_id,
  --                                   tt.form_id,
  --                                   tt.create_time,
  --                                   tt.take_time,
  --                                   tt.end_time,
  --                                   tt.task_state,
  --                                   tt.outgoing_name,
  --                                   tt.opinion,
  --                                   tt.is_first,
  --                                   tt.activity_id,
  --                                   p.cfg_id,
  --                                   p.process_id,
  --                                   p.process_name,
  --                                   p.creator_id,
  --                                   p.title,
  --                                   p.business_id,
  --                                   p.business_type,
  --                                   p.complete_time,
  --                                   (SELECT bb.stru_sname
  --                                      FROM dams_branch bb
  --                                     WHERE bb.stru_id = tt.stru_id) stru_sname,
  --                                   (SELECT uu.name
  --                                      FROM dams_user uu
  --                                     WHERE uu.ssic_id = tt.ssic_id) user_name,
  --                                   (SELECT u.name
  --                                      FROM dams_user u
  --                                     WHERE u.ssic_id = tt.prev_ssic_id) prev_user_name,
  --                                   tt.action_flag action_flag,
  --                                   tt.task_name,
  --                                   decode(tt.prev_task_id, b.pre_task_id, 'start',
  --                                          '') start_point,
  --                                   tt.ssic_id f_ssic_id
  --                              FROM dams_wf_task    tt,
  --                                   dams_wf_process p,
  --                                   buss_table      b
  --                             WHERE tt.business_id = b.business_id
  --                               AND tt.procinst_id = p.procinst_id
  --                            UNION ALL
  --                            SELECT tt.ssic_id,
  --                                   tt.task_id,
  --                                   tt.procinst_id,
  --                                   tt.role_id,
  --                                   tt.stru_id,
  --                                   tt.prev_task_id,
  --                                   tt.prev_ssic_id,
  --                                   tt.prev_stru_id,
  --                                   tt.form_id,
  --                                   tt.create_time,
  --                                   tt.take_time,
  --                                   tt.end_time,
  --                                   tt.task_state,
  --                                   tt.outgoing_name,
  --                                   tt.opinion,
  --                                   tt.is_first,
  --                                   tt.activity_id,
  --                                   p.cfg_id,
  --                                   p.process_id,
  --                                   p.process_name,
  --                                   p.creator_id,
  --                                   p.title,
  --                                   p.business_id,
  --                                   p.business_type,
  --                                   p.complete_time,
  --                                   (SELECT bb.stru_sname
  --                                      FROM dams_branch bb
  --                                     WHERE bb.stru_id = tt.stru_id) stru_sname,
  --                                   (SELECT uu.name
  --                                      FROM dams_user uu
  --                                     WHERE uu.ssic_id = tt.ssic_id) user_name,
  --                                   (SELECT u.name
  --                                      FROM dams_user u
  --                                     WHERE u.ssic_id = tt.prev_ssic_id) prev_user_name,
  --                                   tt.action_flag action_flag,
  --                                   tt.task_name,
  --                                   decode(tt.prev_task_id, b.pre_task_id, 'start',
  --                                          '') start_point,
  --                                   tt.ssic_id f_ssic_id
  --                              FROM dams_wf_hist_task    tt,
  --                                   dams_wf_hist_process p,
  --                                   buss_table           b
  --                             WHERE tt.business_id = b.business_id
  --                               AND tt.procinst_id = p.procinst_id) t,
  --                           (SELECT t1.ssic_id, --dams_conn(t1.ssic_id) ssic_id,
  --                                   u1.name, --dams_conn(u1.name) user_name,
  --                                   b1.stru_id, --dams_conn(b1.stru_id) stru_id,
  --                                   b1.stru_sname, --dams_conn(b1.stru_sname) branch_name,
  --                                   t1.pre_task_id,
  --                                   t1.procinst_id
  --                              FROM dams_wf_participation t1,
  --                                   dams_user             u1,
  --                                   dams_branch           b1,
  --                                   buss_table            bs
  --                             WHERE t1.ssic_id = u1.ssic_id
  --                               AND t1.stru_id = b1.stru_id(+)
  --                               AND t1.business_id = bs.business_id
  --                             GROUP BY t1.pre_task_id, t1.procinst_id) subt,
  --                           dams_wf_activity_alias a
  --                     WHERE t.prev_task_id = subt.pre_task_id(+)
  --                       AND t.procinst_id = subt.procinst_id(+)
  --                       AND t.cfg_id = a.cfgid(+)
  --                       AND t.outgoing_name = a.incoming_transition(+)
  --                       AND t.activity_id = a.activity_id(+)
  --                    UNION ALL
  --                    SELECT NULL task_id,
  --                           '会办人员处理' task_name,
  --                           au.assist_user_id,
  --                           du.name user_name,
  --                           au.stru_id AS stru_id,
  --                           (SELECT bb.stru_sname
  --                              FROM dams_branch bb
  --                             WHERE bb.stru_id = au.stru_id) stru_sname,
  --                           au.task_id prev_task_id,
  --                           iu.prev_ssic_id prev_ssic_id,
  --                           (SELECT iuu.name
  --                              FROM dams_user iuu
  --                             WHERE iuu.ssic_id = iu.prev_ssic_id) prev_user_name,
  --                           iu.creator_id starter_ssic_id,
  --                           '' prev_stru_id,
  --                           '' procinst_id,
  --                           '' process_id,
  --                           '' process_name,
  --                           '' form_id,
  --                           iu.business_type business_type,
  --                           au.business_id,
  --                           iu.title title,
  --                           iu.create_time,
  --                           iu.take_time,
  --                           iu.end_time,
  --                           NULL complete_time,
  --                           iu.task_state,
  --                           decode(au.status, '1', '会办完成') outgoing_name,
  --                           au.assist_user_comment opinion,
  --                           'a' action_name,
  --                           du.name user_name2,
  --                           '' start_point,
  --                           au.assist_user_id f_ssic_id,
  --                           '0' is_first
  --                      FROM dams_assist_user        au,
  --                           dams_user               du,
  --                           viw_dams_inner_all_task iu,
  --                           buss_table              bt
  --                     WHERE au.assist_user_id = du.ssic_id(+)
  --                       AND au.business_id = bt.business_id
  --                       AND decode(au.status, '0', 'open', 'completed') =
  --                           iu.task_state
  --                       AND au.business_id = iu.business_id
  --                       AND iu.task_id = to_char(au.task_id)
  --                       AND au.assist_user_id = iu.ssic_id) rd
  --             START WITH rd.business_id = i_businessid
  --                    AND rd.start_point = 'start'
  --            CONNECT BY rd.prev_task_id = PRIOR rd.task_id
  --             ORDER SIBLINGS BY rd.create_time) f
  --     WHERE (i_tracetype <> '1' OR
  --           (f.is_first <> '1' AND f.f_ssic_id IS NOT NULL))
  --     ORDER BY f.rn;    
  --  pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  --
  --EXCEPTION
  --  WHEN OTHERS THEN
  --    IF o_jbpm_trace%ISOPEN THEN
  --      CLOSE o_jbpm_trace;
  --    END IF;
  --    pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  --    o_flag := '-1';
  --    RETURN;
  --END;
  
  PROCEDURE proc_jbpm_traces_curinfo(i_businessid      IN VARCHAR2, --业务主键
                                     o_flag            OUT VARCHAR2, --存储过程返回标志
                                     o_currenttaskname OUT VARCHAR2, --当前任务名
                                     o_currentusername OUT VARCHAR2 --当前处理人
                                     ) IS
    v_count INTEGER;
  BEGIN
    o_flag := '0';
  
    --逻辑同pckg_dams_task.func_get_cur_info
    --重写了逻辑，支持平台子流程的情况 moded by xiesy 9.29
    SELECT nvl(tmp.name, '无'),
           nvl(decode(tmp.cnt, 1, tmp.latest_task_name,
                      (SELECT MAX(x.outgoing_name) keep(dense_rank LAST ORDER BY x.create_time)
                          FROM dams_wf_task x
                         WHERE x.business_id = tmp.latest_business)), '无')
      INTO o_currentusername, o_currenttaskname
      FROM (SELECT COUNT(DISTINCT t.business_id) cnt,
                   MAX(sp.source_business_id) keep(dense_rank LAST ORDER BY sp.lv) latest_business,
                   MAX(t.task_name) keep(dense_rank LAST ORDER BY t.create_time) latest_task_name,
                   listagg(decode(p.auth_state, '1',
                                  nvl2(b.stru_sname,
                                        u1.name || '[' || b.stru_sname || ']' || '，' ||
                                         u2.name || '转授权',
                                        u1.name || '，' || u2.name || '转授权'),
                                  nvl2(b.stru_sname,
                                        u1.name || '[' || b.stru_sname || ']',
                                        u1.name)), ',') within GROUP(ORDER BY u1.name) NAME
              FROM dams_wf_task t,
                   dams_wf_participation p,
                   dams_user u1,
                   dams_user u2,
                   dams_branch b,
                   (SELECT r.business_id, LEVEL lv, r.source_business_id
                      FROM dams_wf_sub_process_rel r
                     START WITH r.source_business_id = i_businessid
                    CONNECT BY PRIOR r.business_id = r.source_business_id
                    UNION ALL
                    SELECT i_businessid, 0 lv, '' FROM dual) sp
             WHERE t.business_id = sp.business_id
               AND t.task_state = 'open'
               AND t.prev_task_id = p.pre_task_id
               AND t.procinst_id = p.procinst_id
               AND t.business_id = p.business_id
               AND p.ssic_id = u1.ssic_id(+)
               AND p.ssic_id_tran = u2.ssic_id(+)
               AND p.stru_id = b.stru_id(+)) tmp;
    IF (o_currenttaskname = '财务受理岗') THEN
      o_currentusername := '财务受理岗';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      o_flag            := '-1';
      o_currenttaskname := '';
      o_currentusername := '';
  END proc_jbpm_traces_curinfo;
  
  PROCEDURE proc_jbpm_get_retrieve_taskid(i_businessid   IN VARCHAR2,
                                          i_userid       IN VARCHAR2,
                                          o_retcode      OUT VARCHAR2,
                                          o_isflag       OUT VARCHAR2,
                                          o_retaskid     OUT VARCHAR2,
                                          o_retaskname   OUT VARCHAR2,
                                          o_reactivityid OUT VARCHAR2,
                                          o_reroleid     OUT VARCHAR2,
                                          o_restruid     OUT VARCHAR2,
                                          o_curtaskid    OUT VARCHAR2,
                                          o_formid       OUT VARCHAR2,
                                          o_syscode      OUT VARCHAR2) IS
    v_param      db_log.info%TYPE := i_businessid || '|' || i_userid;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_get_retrieve_taskid';
    v_step       db_log.step_no%TYPE;
    v_sql        VARCHAR2(4000);
    v_re_taskid  NUMBER(19);
    v_cur_taskid NUMBER(19);
    v_cnt        INTEGER;
  BEGIN
    o_retcode := '0';
    o_isflag  := '0';
  
    SELECT MAX(t.task_id) keep(dense_rank LAST ORDER BY t.create_time)
      INTO v_re_taskid
      FROM dams_wf_task t
     WHERE t.business_id = i_businessid
       AND t.ssic_id = i_userid;
  
    BEGIN
      SELECT t.task_id
        INTO v_cur_taskid
        FROM dams_wf_task t
       WHERE t.prev_task_id = v_re_taskid
         AND t.business_id = i_businessid
         AND t.task_state = 'open'
         AND t.take_time IS NULL;
    EXCEPTION
      WHEN no_data_found THEN
        --o_retcode := '0';
        o_isflag := '0';
        RETURN;
      WHEN OTHERS THEN
        o_retcode := '-2';
        RETURN;
    END;
  
    SELECT COUNT(1)
      INTO v_cnt
      FROM dams_wf_task re
     WHERE re.task_id = v_re_taskid
       AND re.action_flag = '9'
       AND re.ssic_id = re.prev_ssic_id
       AND re.ssic_id = i_userid;
    IF v_cnt > 0 THEN
      o_isflag := '0';
      RETURN;
    END IF;
  
    SELECT re.task_name,
           re.activity_id,
           re.role_id,
           re.stru_id,
           cur.task_id,
           re.form_id,
           cur.sys_code
      INTO o_retaskname,
           o_reactivityid,
           o_reroleid,
           o_restruid,
           o_curtaskid,
           o_formid,
           o_syscode
      FROM dams_wf_task re, dams_wf_task cur
     WHERE cur.prev_task_id = re.task_id
       AND re.task_id = v_re_taskid;
  
    o_isflag := '1';
  
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_jbpm_get_retrieve_taskid;
  
  PROCEDURE proc_jbpm_taskassignees(i_ssicid              IN VARCHAR2, --用户id
                                    i_cfgid               IN VARCHAR2, --配置ID
                                    i_pdid                IN VARCHAR2, --流程定义ID
                                    i_execute_id          IN VARCHAR2, --流程实例ID
                                    i_activity_id         IN VARCHAR2, --下一活动id
                                    i_task_name           IN VARCHAR2, --下一活动名称
                                    i_transition_name     IN VARCHAR2, --流入的动作名称
                                    i_role_id             IN VARCHAR2, --处理人权限
                                    i_stru_id             IN VARCHAR2, --处理人权限范围
                                    i_sys_code            IN VARCHAR2, --系统编号
                                    i_language            IN VARCHAR2, --语种
                                    i_jbpm_assignee_param IN VARCHAR2, --下一处理人扩展参数
                                    o_flag                OUT VARCHAR2, --存储过程返回标志
                                    o_cacu_type           OUT VARCHAR2, --1.单选  2.会签  3.竞争  4.多路分叉
                                    o_show_type           OUT VARCHAR2, --展现类型 1.到人 2.到机构 3.到角色
                                    o_multiuser_flag      OUT VARCHAR2, --可扩展字段
                                    o_mail_type           OUT VARCHAR2, --邮件类型
                                    o_ext_interface       OUT VARCHAR2, --使用的java处理类
                                    o_jbpm_task_assignee  OUT SYS_REFCURSOR --任务参与者
                                    ) IS
    v_stru_id_array      dams_arrytype;
    v_param              db_log.info%TYPE := i_ssicid || '|' || i_cfgid || '|' ||
                                             i_pdid || '|' || i_execute_id || '|' ||
                                             i_activity_id || '|' ||
                                             i_task_name || '|' ||
                                             i_transition_name || '|' ||
                                             i_role_id || '|' || i_stru_id || '|' ||
                                             i_sys_code || '|' || i_language;
    v_proc_name          db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.PROC_JBPM_TASKASSIGNEES';
    v_step               db_log.step_no%TYPE;
    v_task_name_array    dams_arrytype;
    v_assignee_tuple     dams_wf_assignee_mid%ROWTYPE;
    v_jbpm_task_assignee SYS_REFCURSOR;
  BEGIN
    "$log"(193);
	o_flag := '0';
  
    "$log"(194);
	o_ext_interface := '';
  
    "$log"(195);
	pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
    --如果taskName有，分割，则说明同时有多个下一任务要送达，此场景必为多路分叉场景
    "$log"(196);v_task_name_array := pckg_dams_util.func_str_to_array(i_task_name, ',', '1');
    --清空临时表
    "$log"(197);DELETE FROM dams_wf_assignee_mid;
    --按照下一任务拆分成多个子任务
    "$log"(198);FOR idx IN 1 .. v_task_name_array.count
    LOOP
      "$log"(199);
      "$log"(200);
	  BEGIN
        "$log"(201);
		  IF o_ext_interface IS NULL AND v_jbpm_task_assignee IS NOT NULL THEN
          "$log"(202);
		  FOR j IN 1 .. 1 LOOP
            -- fake loop to simulate 'continue'
            "$log"(203);
			LOOP
              -- inner loop to manipulate assignee data
              "$log"(204);
			  FETCH v_jbpm_task_assignee INTO v_assignee_tuple;
              "$log"(205);
			  EXIT WHEN v_jbpm_task_assignee%NOTFOUND;
              "$log"(206);
			  INSERT INTO dams_wf_assignee_mid
                (ssic_id,
                 user_name,
                 stru_id,
                 stru_sname,
                 role_id,
                 role_name,
                 ssic_id_tran,
                 user_tran_name,
                 auth_state,
                 dest_activity_id,
                 dest_activity_name)
              VALUES
                (v_assignee_tuple.ssic_id,
                 v_assignee_tuple.user_name,
                 v_assignee_tuple.stru_id,
                 v_assignee_tuple.stru_sname,
                 v_assignee_tuple.role_id,
                 v_assignee_tuple.role_name,
                 v_assignee_tuple.ssic_id_tran,
                 v_assignee_tuple.user_tran_name,
                 v_assignee_tuple.auth_state,
                 v_assignee_tuple.dest_activity_id,
                 v_assignee_tuple.dest_activity_name);
            END LOOP;
          END LOOP;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          "$log"(207);o_flag := '-1';
          "$log"(208);pack_log.error(v_proc_name, pack_log.end_step,
                         '异常！任务名:' || v_task_name_array(idx) || v_param);
      END;
    
    END LOOP;
  
    --外部返回
    "$log"(209);IF v_task_name_array.count > 1 THEN
      "$log"(210);o_cacu_type := '4'; --多个任务即fork场景
      "$log"(211);o_show_type := '3'; --fork场景只展现角色
      "$log"(212);o_mail_type := '1'; --暂定都要发邮件
    END IF;
  
    -- 处理重名
    "$log"(213);UPDATE dams_wf_assignee_mid mid
       SET mid.user_name = mid.user_name || '(' || mid.stru_sname ||
                           mid.role_name || ')'
     WHERE nvl(mid.user_tran_name, mid.user_name) IN
           (SELECT nvl(t.user_tran_name, t.user_name)
              FROM dams_wf_assignee_mid t
             GROUP BY nvl(t.user_tran_name, t.user_name)
            HAVING COUNT(1) > 1);
  
    "$log"(214);OPEN o_jbpm_task_assignee FOR
      SELECT t.ssic_id,
             t.user_name,
             t.stru_id,
             t.stru_sname,
             t.role_id,
             t.role_name,
             t.ssic_id_tran,
             t.user_tran_name,
             t.auth_state,
             t.dest_activity_id,
             t.dest_activity_name
        FROM dams_wf_assignee_mid t;
  
    "$log"(215);pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(216);IF o_jbpm_task_assignee%ISOPEN THEN
        "$log"(217);CLOSE o_jbpm_task_assignee;
      END IF;
      "$log"(218);
	  o_flag := '-1';
      "$log"(219);
	  pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_jbpm_taskassignees;
  
  PROCEDURE proc_jbpm_role(i_cfgid     IN VARCHAR2, --配置ID
                           i_pdid      IN VARCHAR2, --流程定义ID
                           i_proincid  IN VARCHAR2, --流程实例ID
                           i_task_id   IN VARCHAR2, --任务ID
                           i_task_name IN VARCHAR2, --当前活动名称
                           i_ssic_id   IN VARCHAR2, --当前处理人
                           i_stru_id   IN VARCHAR2, --传入申请机构id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_role_id   OUT VARCHAR2, --角色id
                           o_stru_id   OUT VARCHAR2 --机构id
                           ) IS
    v_next_activity_id VARCHAR2(255); --活动定义ID
    e_actcfg_notexist      EXCEPTION;
    e_stru_not_in_damsuser EXCEPTION;
  
    v_param     db_log.info%TYPE := i_cfgid || '|' || i_pdid || '|' ||
                                    i_proincid || '|' || i_task_id || '|' ||
                                    i_task_name || '|' || i_ssic_id || '|' ||
                                    i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_role';
    v_step      db_log.step_no%TYPE;
  BEGIN
    o_flag := '0';
  
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  
    IF i_stru_id IS NULL THEN
      --传入机构为空，则审批状态, 从处理人扩展表取
      SELECT p.role_id, p.stru_id
        INTO o_role_id, o_stru_id
        FROM dams_wf_participation p, dams_wf_task t
       WHERE p.pre_task_id = t.prev_task_id
         AND t.task_id = i_task_id
         AND p.ssic_id = i_ssic_id;
    ELSE
      --传入机构不为空，则提交状态
      --流程配置表获得活动id
    
      o_stru_id := i_stru_id;
    
      BEGIN
        SELECT id
          INTO v_next_activity_id
          FROM dams_wf_activity_conf
         WHERE cfgid = i_cfgid
           AND pdid = i_pdid
           AND activity_name = i_task_name;
      EXCEPTION
        WHEN no_data_found THEN
          RAISE e_actcfg_notexist;
      END;
    
      --活动用户表中查找角色
    
      SELECT MAX(t.entity_id)
        INTO o_role_id
        FROM dams_wf_activity_user t
       WHERE t.cfgid = i_cfgid
         AND t.activity_id = v_next_activity_id
         AND t.user_type = 'R'
         AND rownum = 1;
    
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  
  EXCEPTION
    WHEN e_actcfg_notexist THEN
      o_flag := '1';
      RETURN;
    WHEN e_stru_not_in_damsuser THEN
      o_flag := '2';
      RETURN;
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_jbpm_role;
  
  PROCEDURE proc_jbpm_prev_audit_info(i_task_id        IN VARCHAR2, --流程定义ID
                                      o_flag           OUT VARCHAR2, --存储过程返回标志
                                      o_prev_auditinfo OUT SYS_REFCURSOR --上一步处理人信息
                                      ) IS
    v_param     db_log.info%TYPE := i_task_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_prev_audit_info';
    v_step      db_log.step_no%TYPE;
  
  BEGIN
    o_flag := '0';
  
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  
    -- 根据task_id获取
    OPEN o_prev_auditinfo FOR
      SELECT t.prev_task_id, t.prev_ssic_id, t.prev_role_id
        FROM dams_wf_task t
       WHERE t.task_id = i_task_id;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      IF o_prev_auditinfo%ISOPEN THEN
        CLOSE o_prev_auditinfo;
      END IF;
    
      o_flag := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_jbpm_prev_audit_info;
  
  PROCEDURE proc_jbpm_activity_alias(i_pdid       IN VARCHAR2, --流程定义ID
                                     i_cfgid      IN VARCHAR2, --配置ID
                                     i_transition IN VARCHAR2, --流入路径名
                                     i_originname IN VARCHAR2, --图上活动名称
                                     o_flag       OUT VARCHAR2, --存储过程返回标志
                                     o_aliasname  OUT VARCHAR2 --别名
                                     ) IS
    v_param     db_log.info%TYPE := i_pdid || '|' || i_cfgid || '|' ||
                                    i_transition || '|' || i_originname;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_activity_alias';
    v_step      db_log.step_no%TYPE;
  
  BEGIN
    o_flag := '0';
  
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  
    SELECT t.activity_name_alias
      INTO o_aliasname
      FROM dams_wf_activity_conf c, dams_wf_activity_alias t
     WHERE c.cfgid = t.cfgid
       AND c.id = t.activity_id
       AND c.cfgid = i_cfgid
       AND c.pdid = i_pdid
       AND c.activity_name = i_originname
       AND t.incoming_transition = i_transition;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN no_data_found THEN
      o_flag      := '-1';
      o_aliasname := i_originname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
    WHEN OTHERS THEN
      o_flag      := '-1';
      o_aliasname := i_originname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_jbpm_activity_alias;
  
  PROCEDURE proc_jbpm_activity_origin(i_pdid       IN VARCHAR2, --流程定义ID
                                      i_cfgid      IN VARCHAR2, --配置项ID
                                      i_transition IN VARCHAR2, --流入路径名
                                      i_aliasname  IN VARCHAR2, --别名
                                      o_flag       OUT VARCHAR2, --存储过程返回标志
                                      o_originname OUT VARCHAR2 --图上活动名称
                                      ) IS
    v_param     db_log.info%TYPE := i_pdid || '|' || i_cfgid || '|' ||
                                    i_transition || '|' || i_aliasname;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_activity_origin';
    v_step      db_log.step_no%TYPE;
  
  BEGIN
    o_flag := '0';
  
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  
    SELECT c.activity_name
      INTO o_originname
      FROM dams_wf_activity_conf c, dams_wf_activity_alias t
     WHERE c.cfgid = t.cfgid
       AND c.id = t.activity_id
       AND c.cfgid = i_cfgid
       AND c.pdid = i_pdid
       AND t.activity_name_alias = i_aliasname
       AND t.incoming_transition = i_transition;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN no_data_found THEN
      o_flag       := '-1';
      o_originname := i_aliasname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
    WHEN OTHERS THEN
      o_flag       := '-1';
      o_originname := i_aliasname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_jbpm_activity_origin;
  
  PROCEDURE proc_taskqry_open(i_businessid IN VARCHAR2,
                              i_user_id    IN VARCHAR2,
                              i_type       IN VARCHAR2, --1只查询自身参与过的文,0不限制
                              i_sys_code   IN VARCHAR2,
                              o_flag       OUT VARCHAR2,
                              o_state      OUT VARCHAR2, --todo代办done已办over完成
                              o_task_id    OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_jbpm.proc_taskqry_open';
    v_param     db_log.info%TYPE := i_businessid || '#' || i_user_id || '#' ||
                                    i_sys_code;
    v_step      db_log.step_no%TYPE;
    v_task      dams_wf_task.task_id%TYPE := NULL;
    v_count     NUMBER(10) := 0;
    v_type      VARCHAR2(1) := '';
  BEGIN
    o_flag := '0';
    --如为空则默认为1只查自身参与过的文
    v_type := nvl(i_type, '1');
    IF v_type = '0' THEN
      --先查询是否在正常流程表中，如没有则查找历史流程表
      SELECT COUNT(1)
        INTO v_count
        FROM dams_wf_task t
       WHERE t.business_id = i_businessid
         AND t.sys_code = i_sys_code
         AND rownum = 1;
      IF v_count > 0 THEN
        BEGIN
          --找当前文是否有自己的待办
          SELECT t.task_id
            INTO v_task
            FROM dams_wf_task t, jbpm4_participation p
           WHERE t.business_id = i_businessid
             AND t.task_state = 'open'
             AND t.task_id = p.task_
             AND p.userid_ = i_user_id
             AND rownum = 1;
          o_state := 'todo';
        EXCEPTION
          WHEN OTHERS THEN
            --没有自己的待办，则查已办
            SELECT data.task_id
              INTO v_task
              FROM (SELECT t.task_id,
                           row_number() over(ORDER BY t.create_time DESC) rn
                      FROM dams_wf_task t
                     WHERE t.business_id = i_businessid
                          --AND t.ssic_id = i_user_id
                       AND t.task_id > 0) data
             WHERE data.rn = 1;
            o_state := 'done';
        END;
        o_task_id := v_task;
      ELSE
        SELECT COUNT(1)
          INTO v_count
          FROM dams_wf_hist_task t
         WHERE t.business_id = i_businessid
           AND t.sys_code = i_sys_code
           AND rownum = 1;
        IF v_count > 0 THEN
          --查询已完成的taskid
          SELECT data.task_id
            INTO v_task
            FROM (SELECT t.task_id,
                         row_number() over(ORDER BY t.create_time DESC) rn
                    FROM dams_wf_hist_task t
                   WHERE t.business_id = i_businessid
                  --AND t.ssic_id = i_user_id
                  ) data
           WHERE data.rn = 1;
          o_state   := 'over';
          o_task_id := v_task;
        ELSE
          o_flag := '1';
          RETURN;
        END IF;
      END IF;
    ELSE
      --先查询是否在正常流程表中，如没有则查找历史流程表
      SELECT COUNT(1)
        INTO v_count
        FROM dams_wf_task t
       WHERE t.business_id = i_businessid
         AND t.sys_code = i_sys_code
         AND rownum = 1;
      IF v_count > 0 THEN
        BEGIN
          --找当前文是否有自己的待办
          SELECT t.task_id
            INTO v_task
            FROM dams_wf_task t, jbpm4_participation p
           WHERE t.business_id = i_businessid
             AND t.task_state = 'open'
             AND t.task_id = p.task_
             AND p.userid_ = i_user_id
             AND rownum = 1;
          o_state := 'todo';
        EXCEPTION
          WHEN OTHERS THEN
            --没有自己的待办，则查自己的已办
            SELECT data.task_id
              INTO v_task
              FROM (SELECT t.task_id,
                           row_number() over(ORDER BY t.create_time DESC) rn
                      FROM dams_wf_task t
                     WHERE t.business_id = i_businessid
                       AND t.ssic_id = i_user_id
                       AND t.task_id > 0) data
             WHERE data.rn = 1;
            o_state := 'done';
        END;
        o_task_id := v_task;
      ELSE
        SELECT COUNT(1)
          INTO v_count
          FROM dams_wf_hist_task t
         WHERE t.business_id = i_businessid
           AND t.sys_code = i_sys_code
           AND rownum = 1;
        IF v_count > 0 THEN
          --查询自己已完成的taskid
          SELECT data.task_id
            INTO v_task
            FROM (SELECT t.task_id,
                         row_number() over(ORDER BY t.create_time DESC) rn
                    FROM dams_wf_hist_task t
                   WHERE t.business_id = i_businessid
                     AND t.ssic_id = i_user_id) data
           WHERE data.rn = 1;
          o_state   := 'over';
          o_task_id := v_task;
        ELSE
          o_flag := '1';
          RETURN;
        END IF;
      END IF;
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
  END proc_taskqry_open;
  
  --取回（已处理过的环节处理人都可取回）
  PROCEDURE proc_get_retrieve_info(i_businessid    IN VARCHAR2,
                                   i_userid        IN VARCHAR2,
                                   o_retcode       OUT VARCHAR2,
                                   o_msg           OUT VARCHAR2,
                                   o_isflag        OUT VARCHAR2,
                                   o_retrieve_info OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_businessid || '|' || i_userid;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_get_retrieve_info';
    v_step       db_log.step_no%TYPE;
    v_sql        VARCHAR2(4000);
    v_re_taskid  NUMBER(19);
    v_cur_taskid NUMBER(19);
    v_cnt        INTEGER := 0;
  BEGIN
    o_retcode := '0';
    o_msg     := '';
    o_isflag  := '0';
  
    SELECT MAX(t.task_id) keep(dense_rank LAST ORDER BY t.create_time)
      INTO v_re_taskid
      FROM dams_wf_task t
     WHERE t.business_id = i_businessid
       AND t.ssic_id = i_userid
       AND t.task_state = 'completed';
  
    SELECT COUNT(1)
      INTO v_cnt
      FROM dams_wf_task t
     WHERE t.business_id = i_businessid
       AND t.task_state = 'open';
    IF v_cnt > 1 THEN
      o_isflag := '0';
      o_msg    := '该申请当前有多条待办，系统不支持取回';
      RETURN;
    END IF;
  
    SELECT COUNT(1)
      INTO v_cnt
      FROM dams_wf_task t, jbpm4_participation p
     WHERE t.business_id = i_businessid
       AND t.task_state = 'open'
       AND t.task_id = p.task_
       AND p.userid_ = i_userid;
    IF v_cnt > 0 THEN
      o_isflag := '0';
      o_msg    := '该申请已经在您的待办工作中，无需取回';
      RETURN;
    END IF;
  
    OPEN o_retrieve_info FOR
      SELECT re.task_name,
             re.activity_id,
             re.role_id,
             re.stru_id,
             (SELECT t.task_id
                FROM dams_wf_task t
               WHERE t.business_id = i_businessid
                 AND t.task_state = 'open'
                 AND rownum = 1) curtaskid,
             re.form_id,
             re.sys_code
        FROM dams_wf_task re
       WHERE re.task_id = v_re_taskid;
    o_isflag := '1';
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
      o_msg     := '该申请取回发生异常，请稍后操作';
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
  END proc_get_retrieve_info;
  
END pckg_dams_jbpm;
//

CREATE OR REPLACE PACKAGE pckg_dams_util2 IS
  PROCEDURE proc_get_st_pdkey(i_stru_id  IN VARCHAR2, --机构
                             i_sec_lv   IN VARCHAR2, --密级
                             i_dsm_type IN VARCHAR2, --涉密文档提供类型：1复制 2对外提供
                             o_ret_code OUT VARCHAR2,
                             o_msg      OUT VARCHAR2,
                             o_pdkey    OUT VARCHAR2,
                             o_biztype  OUT VARCHAR2);
  
END pckg_dams_util2;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_util2 IS
	"$hits" aaa_coverage_tool.bool_tab; 
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit; 
    end;
	  begin
	   if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(5);
	   end if;
	   if "$times" is null then 
		 --"$times" := aaa_coverage_tool.get_statement_time_tab(5);
		 dbms_output.put_line('There is no such letter get_statement_time_tab in aaa_coverage_tool');
	   end if;
	   if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(5,x); 
		"$hits"(x):=true; 
	   end if;
	   if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(5,x); 
		"$times"(x):=true; 
	   end if;
	end;
	
	PROCEDURE proc_get_st_pdkey(i_stru_id  IN VARCHAR2, --机构
							  i_sec_lv   IN VARCHAR2, --密级
							  i_dsm_type IN VARCHAR2, --涉密文档提供类型：1复制 2对外提供
							  o_ret_code OUT VARCHAR2,
							  o_msg      OUT VARCHAR2,
							  o_pdkey    OUT VARCHAR2,
							  o_biztype  OUT VARCHAR2) IS
	v_param     db_log.info%TYPE := i_stru_id || '|' || i_sec_lv || '|' || i_dsm_type;
	v_proc_name db_log.proc_name%TYPE := 'pckg_dams_util.proc_get_st_pdkey';
	v_step      db_log.step_no%TYPE;
	v_stru_level VARCHAR2(10);
	v_pdkey      VARCHAR2(255);
	v_biztype    VARCHAR2(20);
	v_count      NUMBER(5);
	BEGIN
	  pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg || v_param);
	  o_ret_code := 0;
	  IF i_stru_id IS NULL THEN
	    o_ret_code := -2;
		o_msg      := '请选择您的申请部门！';
		o_pdkey    := '';
	    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
		RETURN;
	  END IF;
	SELECT MAX(t.stru_level)
	  INTO v_stru_level
	  FROM dams_branch_catalog t
	 WHERE t.stru_id = i_stru_id;
	--8  境外机构
	--18 境内控股机构
	SELECT COUNT(1)
	  INTO v_count
	  FROM dams_branch_relation t
	 WHERE t.bnch_id = i_stru_id
	   AND t.bnch_sign IN ('8', '18');
	IF v_count > 0 THEN
	  v_stru_level := '9';
	END IF;
	IF v_stru_level IS NULL THEN
	  o_ret_code := -2;
	  o_msg      := '获取流程PDKEY失败';
	  o_pdkey    := '';
	ELSE
	  IF i_dsm_type = 'dsmcopy' THEN
		v_pdkey := 'stdsmcopy';
	  ELSIF i_dsm_type = 'dsmprovide' THEN
		v_pdkey := 'stdsmprovide';
	  END IF;
	  IF v_stru_level = '1' THEN
		v_pdkey := v_pdkey || '_' || 'head';
	  ELSIF v_stru_level = '2' THEN
		v_pdkey := v_pdkey || '_' || 'bnch';
	  ELSIF v_stru_level = '3' OR v_stru_level = '4' THEN
		v_pdkey := v_pdkey || '_' || 'sub_bnch';
	  ELSIF v_stru_level = '9' THEN
		v_pdkey := v_pdkey || '_' || 'os_bnch';
	  END IF;
	  SELECT COUNT(1)
		INTO v_count
		FROM dams_wf_process_conf t
	   WHERE t.process_key = v_pdkey
		 AND t.sys_code = 'QXSQ'; 
	  SELECT CASE v_pdkey
			   WHEN 'stdsmcopy_head' THEN
				'ST03'
			   WHEN 'stdsmcopy_bnch' THEN
				'ST04'
			   WHEN 'stdsmcopy_sub_bnch' THEN
				'ST05'
			   WHEN 'stdsmcopy_os_bnch' THEN
				'ST06'
			   WHEN 'stdsmprovide_head' THEN
				'ST07'
			   WHEN 'stdsmprovide_bnch' THEN
				'ST08'
			   WHEN 'stdsmprovide_sub_bnch' THEN
				'ST09'
			   WHEN 'stdsmprovide_os_bnch' THEN
				'ST10'
			 END biztype
		INTO v_biztype
		FROM dual; 
	  IF v_count > 0 THEN
		o_pdkey   := v_pdkey;
		o_biztype := v_biztype;
	  END IF;
	END IF;
	pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
	EXCEPTION
	WHEN OTHERS THEN
	  --dbms_output.put_line(SQLERRM);
	  "$log"(340);
	  pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
	  "$log"(341);
	  o_msg      := '获取流程PDKEY失败';
	  "$log"(342);
	  o_pdkey    := '';
	  "$log"(343);
	  o_ret_code := -1;
	END proc_get_st_pdkey;	
	
END pckg_dams_util2;
//


DELIMITER ;//

