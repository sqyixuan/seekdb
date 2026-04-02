--disable_query_log
--disable_result_log
set @trace_id=(select last_trace_id() from dual);
set @request_time=(select request_time from oceanbase.gv$ob_sql_audit where trace_id=@trace_id order by request_time limit 1);
set @elapsed_time=(select elapsed_time from oceanbase.gv$ob_sql_audit where trace_id=@trace_id order by request_time limit 1);
set @request_ts=(select usec_to_time(@request_time) from dual);
set @end_ts=(select usec_to_time(@request_time + @elapsed_time) from dual);
--disable_warnings
drop function if exists check_timemodel;
--enable_warnings
delimiter /;
CREATE FUNCTION `check_timemodel`(
    time_model BIGINT UNSIGNED,
    forbidden_pairs JSON
) RETURNS tinyint(1)
    DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE pair_count INT;
    DECLARE current_pair JSON;
    DECLARE bit1, bit2 TINYINT;
    DECLARE current_mask BIGINT UNSIGNED;

    -- 获取共存对总数
    SET pair_count = JSON_LENGTH(forbidden_pairs);

    -- 遍历所有允许的共存对
    WHILE i < pair_count DO
        -- 提取当前位对
        SET current_pair = JSON_EXTRACT(forbidden_pairs, CONCAT('$[', i, ']'));
        
        -- 解析位对数值
        SET bit1 = CAST(JSON_UNQUOTE(JSON_EXTRACT(current_pair, '$[0]')) AS UNSIGNED);
        SET bit2 = CAST(JSON_UNQUOTE(JSON_EXTRACT(current_pair, '$[1]')) AS UNSIGNED);
        
        -- 生成掩码（位编号从1开始）
        SET current_mask = (1 << (bit1 - 1)) | (1 << (bit2 - 1));
        
        -- 检测是否同时设置这两个位
        IF (time_model & current_mask) = current_mask THEN
            RETURN TRUE;
        END IF;

        SET i = i + 1;
    END WHILE;

    RETURN FALSE;
END
/
delimiter ;/

--enable_query_log
--enable_result_log
--result_format 4
--echo 'check sql audit'
set @audit_stmt_type='NONE';
set @audit_sql_id='';
set @audit_plan_hash=0;
select @audit_sql_id:=sql_id as sql_id, @audit_plan_hash:=plan_hash as plan_hash, @audit_stmt_type:=stmt_type as stmt_type, partition_cnt, affected_rows, return_rows, insert_duplicate_row_count from oceanbase.gv$ob_sql_audit where trace_id=@trace_id order by request_time asc limit 1;
set @query_sql=(select query_sql from oceanbase.gv$ob_sqlstat where sql_id='B7A6FA97FEC98C06F9586D23935AC4C6' and plan_hash=0 and svr_ip=source_ip and svr_port=source_port  and query_sql is not null limit 1);
--echo 'check sql audit max wait event and total wait event time'
select * from oceanbase.gv$ob_sql_audit where trace_id=@trace_id and WAIT_TIME_MICRO>TOTAL_WAIT_TIME_MICRO;
--echo 'check ASH sample time, will output invalid sample time'
select msg from (select case when sample_time>=@request_ts and sample_time<=@end_ts then sample_time ELSE concat('invalid sample time: ', sample_time, ' request_time: ', @request_ts, ' end_time: ', @end_ts) end as msg from oceanbase.gv$ob_active_session_history where trace_id=@trace_id) t where msg like 'invalid sample time%'; 
--echo 'check ASH time model'
set @tm_masks='[[1, 4], [1, 5], [1, 6], [1, 7], [1, 8], [1, 9], [1, 10], [1, 11], [1,15], [1, 16], [1, 18], [1, 19], [1, 20], [1, 23], [3, 4], [3, 5], [3, 7], [3, 8], [3, 9], [3, 10], [3, 11], [3, 15], [4, 5], [4, 6], [4, 8], [4, 9], [4, 10], [4, 11], [4, 12], [4, 16], [5, 8], [5, 16], [5, 21], [5, 22], [9, 10], [9, 21], [9, 22], [10, 21], [10, 22], [21, 23]]';
select * from oceanbase.gv$ob_active_session_history where trace_id=@trace_id and check_timemodel(time_model, @tm_masks)=true;
--echo 'check ASH sql_id/plan_hash/stmt_type whether invalid, will output invalid metrics result'
select * from (select sql_id, plan_hash, case stmt_type when 1 then 'SELECT' when 2 then 'INSERT' when 3 then 'REPLACE' when 4 then 'DELETE' when 5 then 'UPDATE' when 6 then 'MERGE' ELSE 'OTHER' end as stmt_type from oceanbase.gv$ob_active_session_history where trace_id=@trace_id and IN_SQL_EXECUTION='Y') t where (stmt_type != 'OTHER' and stmt_type != @audit_stmt_type) OR sql_id != @audit_sql_id OR plan_hash != @audit_plan_hash;
--echo 'check ASH delta db time, will output invalid meterics result'
WITH session_samples AS (
  SELECT
    session_id,
    sample_time,
    CASE WHEN event_no = 0 THEN 'ON CPU' ELSE event END AS event,
    time_waited,
    tm_delta_time,
    tm_delta_db_time,
    tm_delta_cpu_time,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY sample_time) AS sample_seq
  FROM oceanbase.gv$ob_active_session_history where trace_id=@trace_id
),
triple_samples AS (
  SELECT
    curr.session_id,
    curr.sample_time AS curr_time,
    next.sample_time AS next_time,
    curr.event,
    next.event AS next_event,
    curr.time_waited,
    curr.tm_delta_time,
    curr.tm_delta_db_time,
    curr.tm_delta_cpu_time,
    MAX(TIME_TO_USEC(curr.sample_time)) OVER (PARTITION BY curr.session_id) - MIN(TIME_TO_USEC(curr.sample_time)) OVER (PARTITION BY curr.session_id) AS time_diff_actual,
    SUM(next.tm_delta_time) OVER (PARTITION BY curr.session_id) AS delta_sum,
    SUM(CASE WHEN next.event = 'ON CPU' THEN 1 ELSE 0 END)
      OVER (PARTITION BY curr.session_id) AS cpu_count,
    SUM(next.time_waited) OVER (PARTITION BY curr.session_id) AS total_waited_time,
    SUM(next.tm_delta_cpu_time) OVER (PARTITION BY curr.session_id) AS total_db_cpu,
    SUM(next.tm_delta_db_time) OVER (PARTITION BY curr.session_id) AS total_db_time
  FROM session_samples curr
  LEFT JOIN session_samples next
    ON curr.session_id = next.session_id
    AND curr.sample_seq = next.sample_seq - 1
)
SELECT
  session_id,
  curr_time,
  next_time,
  event,
  next_event,
  time_waited,
  tm_delta_time,
  tm_delta_db_time,
  tm_delta_cpu_time,
  time_diff_actual,
  delta_sum,
  cpu_count,
  total_waited_time,
  total_db_time,
  total_db_cpu
FROM triple_samples
WHERE
  (time_diff_actual>0 AND ABS(time_diff_actual - delta_sum)>100000)
  OR
  (total_waited_time>0 AND ABS(total_db_time - total_db_cpu)<=0)
  OR
  (event<>'ON CPU' AND event<>'exec inner sql wait' AND (next_event='ON CPU' OR next_event IS NULL) and time_waited<=0);
--echo 'check time_model with operator id'
select * from oceanbase.gv$ob_active_session_history where trace_id=@trace_id and IN_SQL_EXECUTION='N' and sql_plan_line_id is not null;
select * from oceanbase.gv$ob_active_session_history where trace_id=@trace_id and IN_STORAGE_WRITE='Y' and stmt_type not in (2, 3, 4, 5, 6) and @query_sql not like '%for update%'; 
--disable_query_log
--disable_result_log
drop function check_timemodel;
--enable_query_log
--enable_result_log
