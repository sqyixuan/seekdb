connection conn1;
#eval SELECT distinct(table_id) FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE table_name='$table_name';

#eval select count(*), avg(k), avg(v1), sum(k), sum(v1) from $table_name;
eval select * from $table_name order by k, v1;
eval explain select * from $table_name order by k, v1;

# wait ddl redo clog replay in follower
sleep 2;
connection conn2;
set ob_route_policy=READONLY_ZONE_FIRST;
set ob_read_consistency = WEAK;
#eval select count(*), avg(k), avg(v1), sum(k), sum(v1) from $table_name;
eval select * from $table_name order by k, v1;
eval explain select * from $table_name order by k, v1;

set ob_route_policy=COLUMN_STORE_ONLY;
set ob_read_consistency = WEAK;
#eval select count(*), avg(k), avg(v1), sum(k), sum(v1) from $table_name;
#eval select * from $table_name order by k, v1;
#eval explain select * from $table_name order by k, v1;
set ob_read_consistency = STRONG;
set ob_route_policy=READONLY_ZONE_FIRST;
connection conn1;
