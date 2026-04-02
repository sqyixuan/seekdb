#
# set $__tenant_id__/$__table_name__ outside
#

--disable_query_log
--disable_result_log

connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;

let $__table_id__=query_get_value(select /*+QUERY_TIMEOUT(10000000)*/ table_id from oceanbase.__all_virtual_table where table_name= $__table_name__, table_id, 1);

let $__major_scn__=query_get_value(select GLOBAL_BROADCAST_SCN from oceanbase.CDB_OB_MAJOR_COMPACTION, GLOBAL_BROADCAST_SCN, 1);

let $cnt=query_get_value(select /*+QUERY_TIMEOUT(10000000)*/ count(*)>0 as cnt from oceanbase.__all_virtual_tablet_compaction_history where tablet_id in(select /*+QUERY_TIMEOUT(10000000)*/tablet_id from oceanbase.__all_virtual_tablet_to_ls where table_id in (select /*+QUERY_TIMEOUT(10000000)*/table_id from oceanbase.__all_virtual_table where data_table_id=$__table_id__)) and type="MAJOR_MERGE" and compaction_scn=$__major_scn__ and mds_filter_info!='' and is_full_merge=1, cnt, 1);
if ($cnt != 1)
{
--echo ERROR !!! major merge should contain truncate info
--echo index_tablet_id=$__index_tablet_id__, compaction_scn=$__major_scn__
--enable_query_log
--enable_result_log
eval select * from oceanbase.__all_virtual_tablet_compaction_history where tablet_id=$__index_tablet_id__ and type="MAJOR_MERGE";
--disable_query_log
--disable_result_log
}

disconnect obsys_ydm;
connection default;
--enable_query_log
--enable_result_log
