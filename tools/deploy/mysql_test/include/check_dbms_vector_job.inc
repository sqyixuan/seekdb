let $database_id = `select database_id from oceanbase.__all_database where database_name = '$shk_database_name';`;
let $table_id = `select table_id from oceanbase.__all_table where table_name = '$shk_table_name' and database_id = $database_id;`;
let $delta_idx_name = `select CONCAT('__idx_', $table_id , '_$shk_vidx_name') from dual;`;
let $delta_idx_id = `select table_id from oceanbase.__all_table where table_name = '$delta_idx_name';`;
let $refresh_job = `select job_action from oceanbase.__all_tenant_scheduler_job where job_action like "%DBMS_VECTOR.refresh_index%" and job != 0 and exec_env is not NULL and job_name like "$delta_idx_id%";`;
let $rebuild_job = `select job_action from oceanbase.__all_tenant_scheduler_job where job_action like "%DBMS_VECTOR.rebuild_index%" and job != 0 and exec_env is not NULL and job_name like "$delta_idx_id%";`;
let $expect_refresh_job = `select CONCAT("DBMS_VECTOR.refresh_index_inner(", $delta_idx_id, ", 10000)") from dual;`;
let $expect_rebuild_job = `select CONCAT("DBMS_VECTOR.rebuild_index_inner(", $delta_idx_id, ", 0.200000)") from dual;`;

if ( $refresh_job == $expect_refresh_job ) 
{
    --echo refresh_job name is OK
}

if ( $rebuild_job == $expect_rebuild_job )
{
    --echo rebuild_job name is OK
}