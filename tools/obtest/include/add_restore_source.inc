--disable_all_result_log
let $restore_mode = deploy_get_value(ob1.get_backup_restore_mode $backup_oracle_tenant);

let $first_piece_id = query_get_value(select BASE_PIECE_ID from CDB_OB_ARCHIVELOG  where tenant_id=$backup_oracle_tenant_id, BASE_PIECE_ID, 1);
let $current_piece_id = query_get_value(select USED_PIECE_ID from CDB_OB_ARCHIVELOG  where tenant_id=$backup_oracle_tenant_id, USED_PIECE_ID, 1);
let $piece_id = $first_piece_id;

#nfs
if ($restore_mode == 1)
{
  while($piece_id <=  $current_piece_id)
  {
      let $str1 = /piece_d;
      let $str2 = r1p;
      let $piece_path = $log_backup_dest$str1$backup_oracle_tenant_id$str2$piece_id;
      eval alter system add restore source "$piece_path";
      inc $piece_id;
  }
  let $backup_set = /backup_set_1_full;
  eval alter system add restore source "$data_backup_dest$backup_set";
  let $backup_set = /backup_set_2_inc;
  eval alter system add restore source "$data_backup_dest$backup_set";
  let $backup_set = /backup_set_3_full;
  eval alter system add restore source "$data_backup_dest$backup_set";

  if ($__only_plus_archivelog__ == 1)
  {
    alter system clear restore source;
    let $backup_set = /backup_set_3_full;
    eval alter system add restore source "$data_backup_dest$backup_set";
  }
}

#cloud storage
if ($restore_mode != 1) 
{
  let $oss_archived_path = `select REGEXP_SUBSTR('$log_backup_dest', '[^?]+', 1, 1)`;
  let $auth_info =  `select REGEXP_SUBSTR('$log_backup_dest', '[^?]+', 1, 2)`;
  let $oss_data_path = `select REGEXP_SUBSTR('$data_backup_dest', '[^?]+', 1, 1)`;
  let $question_mark = ?;
  while($piece_id <=  $current_piece_id)
  {
      let $str1 = /piece_d;
      let $str2 = r1p;
      let $piece_path = $oss_archived_path$str1$backup_oracle_tenant_id$str2$piece_id$question_mark$auth_info;
      eval alter system add restore source "$piece_path";
      inc $piece_id;
  }
  let $backup_set = /backup_set_1_full;
  eval alter system add restore source "$oss_data_path$backup_set$question_mark$auth_info";
  let $backup_set = /backup_set_2_inc;
  eval alter system add restore source "$oss_data_path$backup_set$question_mark$auth_info";
  let $backup_set = /backup_set_3_full;
  eval alter system add restore source "$oss_data_path$backup_set$question_mark$auth_info";

  if ($__only_plus_archivelog__ == 1)
  {
    alter system clear restore source;
    let $backup_set = /backup_set_3_full;
    eval alter system add restore source "$oss_data_path$backup_set$question_mark$auth_info";
  }
}
--enable_all_result_log