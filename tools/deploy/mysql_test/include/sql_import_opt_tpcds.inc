--disable_query_log
--disable_result_log

let $import_already = 0;
#### 检查数据完整性
--error 0,1146,942
let $has_tpcds = query_get_value(select max(1) as NUM from opt_tpcds.dbgen_version, NUM, 1);
if ($has_tpcds == 1)
{
  let $dv_count = query_get_value(select count(*) as NUM from opt_tpcds.dbgen_version, NUM, 1);
  let $ca_count = query_get_value(select count(*) as NUM from opt_tpcds.customer_address, NUM, 1);
  let $cd_count = query_get_value(select count(*) as NUM from opt_tpcds.customer_demographics, NUM, 1);
  let $dd_count = query_get_value(select count(*) as NUM from opt_tpcds.date_dim, NUM, 1);
  let $wa_count = query_get_value(select count(*) as NUM from opt_tpcds.warehouse, NUM, 1);
  let $sm_count = query_get_value(select count(*) as NUM from opt_tpcds.ship_mode, NUM, 1);
  let $td_count = query_get_value(select count(*) as NUM from opt_tpcds.time_dim, NUM, 1);
  let $re_count = query_get_value(select count(*) as NUM from opt_tpcds.reason, NUM, 1);
  let $ib_count = query_get_value(select count(*) as NUM from opt_tpcds.income_band, NUM, 1);
  let $it_count = query_get_value(select count(*) as NUM from opt_tpcds.item, NUM, 1);
  let $st_count = query_get_value(select count(*) as NUM from opt_tpcds.store, NUM, 1);
  let $cc_count = query_get_value(select count(*) as NUM from opt_tpcds.call_center, NUM, 1);
  let $cus_count = query_get_value(select count(*) as NUM from opt_tpcds.customer, NUM, 1);
  let $ws_count = query_get_value(select count(*) as NUM from opt_tpcds.web_site, NUM, 1);
  let $sr_count = query_get_value(select count(*) as NUM from opt_tpcds.store_returns, NUM, 1);
  let $hd_count = query_get_value(select count(*) as NUM from opt_tpcds.household_demographics, NUM, 1);
  let $wp_count = query_get_value(select count(*) as NUM from opt_tpcds.web_page, NUM, 1);
  let $pr_count = query_get_value(select count(*) as NUM from opt_tpcds.promotion, NUM, 1);
  let $cp_count = query_get_value(select count(*) as NUM from opt_tpcds.catalog_page, NUM, 1);
  let $in_count = query_get_value(select count(*) as NUM from opt_tpcds.inventory, NUM, 1);
  let $cr_count = query_get_value(select count(*) as NUM from opt_tpcds.catalog_returns, NUM, 1);
  let $wr_count = query_get_value(select count(*) as NUM from opt_tpcds.web_returns, NUM, 1);
  let $ws_count = query_get_value(select count(*) as NUM from opt_tpcds.web_sales, NUM, 1);
  let $cs_count = query_get_value(select count(*) as NUM from opt_tpcds.catalog_sales, NUM, 1);
  let $ss_count = query_get_value(select count(*) as NUM from opt_tpcds.store_sales, NUM, 1);

  if ($dv_count == 1 && $ca_count == 500 && $cd_count == 8000 && $dd_count == 700
      && $wa_count == 5 && $sm_count == 20 && $td_count == 800 && $re_count == 35
      && $ib_count == 20 && $it_count == 200 && $st_count == 12 && $cc_count == 6
      && $cus_count == 1000 && $ws_count == 30 && $sr_count == 3000 && $hd_count == 200
      && $wp_count == 60 && $pr_count == 300 && $cp_count == 300 && $in_count == 8000
      && $cr_count == 1500 && $wr_count == 700 && $ws_count == 3500 && $cs_count == 4000 && $ss_count = 5000) {
    let $import_already = 1;
  }
}

#### 如果数据不完整则重新导入
if($import_already == 0)
{
    alter system set minor_compact_trigger = 1;
    --source mysql_test/include/sql_import_opt_tpcds_internal.inc
    --source mysql_test/include/minorfreeze.inc
    --source mysql_test/include/wait_minor_merge.inc
    --source mysql_test/include/sql_import_opt_tpcds_wait_minor.inc

    --disable_query_log
    connection default;
    call dbms_stats.gather_schema_stats('OPT_TPCDS', method_opt=>'FOR ALL COLUMNS SIZE AUTO');
    alter system set minor_compact_trigger = 2;
}

--enable_query_log
--enable_result_log
