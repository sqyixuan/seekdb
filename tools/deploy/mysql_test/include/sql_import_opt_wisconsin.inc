--disable_query_log
--disable_result_log

let $db_name = opt_wisconsin;
if ($wisconsin_db_name!= '') {
  let $db_name = $wisconsin_db_name;
}

#### 检查数据是否导入完整
let $import_already=0;

--error 0,1146,942
let $has_db = query_get_value(select max(1) as NUM from $db_name.g_4k, NUM, 1);

if ($has_db == 1)
{
  let $g_4k_count = query_get_value(select count(*) as NUM from $db_name.g_4k, NUM, 1);
  let $h_4k_count = query_get_value(select count(*) as NUM from $db_name.h_4k, NUM, 1);
  let $t1_1_count = query_get_value(select count(*) as NUM from $db_name.t1_1, NUM, 1);
  let $t1_100_count = query_get_value(select count(*) as NUM from $db_name.t1_100, NUM, 1);
  let $t2_1_count = query_get_value(select count(*) as NUM from $db_name.t2_1, NUM, 1);
  let $t2_100_count = query_get_value(select count(*) as NUM from $db_name.t2_100, NUM, 1);
  let $t_10k_count = query_get_value(select count(*) as NUM from $db_name.t_10k, NUM, 1);
  let $t_4k_count = query_get_value(select count(*) as NUM from $db_name.t_4k, NUM, 1);
  let $t_5k_count = query_get_value(select count(*) as NUM from $db_name.t_5k, NUM, 1);
  let $t_part_count = query_get_value(select count(*) as NUM from $db_name.t_part, NUM, 1);
  let $z_4k_count = query_get_value(select count(*) as NUM from $db_name.z_4k, NUM, 1);

  if ($g_4k_count   == 4000 &&
      $h_4k_count   == 4000 &&
      $t1_1_count   == 1 &&
      $t1_100_count == 100 &&
      $t2_1_count   == 1 &&
      $t2_100_count == 100 &&
      $t_10k_count  == 10000 &&
      $t_4k_count   == 4000 &&
      $t_5k_count   == 5000 &&
      $t_part_count == 9000 &&
      $z_4k_count   == 4000) {
    let $import_already = 1;
  }
}

if ($import_already == 0)
{
  if ($TENANT =='mysql') {
    --source mysql_test/include/sql_import_opt_wisconsin_internal_sch.inc
    --source mysql_test/include/sql_import_opt_wisconsin_internal.inc
  }
  if ($TENANT =='oracle') {
    --source mysql_test/include/sql_import_opt_wisconsin_internal_sch.inc
    --source mysql_test/include/sql_import_opt_wisconsin_internal_oracle.inc
  }
  alter system set minor_compact_trigger = 1;
  --source mysql_test/include/minor_merge_tenant.inc
  --disable_query_log
  alter system set minor_compact_trigger = 2;
  call dbms_stats.gather_schema_stats('OPT_WISCONSIN', method_opt=>'FOR ALL COLUMNS SIZE AUTO');
}

--enable_query_log
--enable_result_log
