--disable_query_log
# Cryptic variable name to avoid name conflicts
set @previous_sql_mode_htnt542nh=@@sql_mode;
####OB��֧�������﷨
set sql_mode=(select replace(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
set @sql_test_mode=replace(@@sql_mode,'ONLY_FULL_GROUP_BY','');
set sql_mode= @sql_test_mode;
--enable_query_log

