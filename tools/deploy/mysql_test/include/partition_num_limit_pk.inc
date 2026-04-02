--disable_warnings
drop table if exists t1;
--enable_warnings
let $create_stmt = CREATE TABLE t1 (
	col1 INT primary key, 
	col2 VARCHAR(20), 
	col3 BIGINT  
;

while ($index_cnt)
{
	let $create_stmt = $create_stmt , INDEX (col1 , col2) LOCAL;
	dec $index_cnt;
}

while ($unique_cnt)
{
	let $create_stmt = $create_stmt , UNIQUE INDEX (col1 , col2) LOCAL;
	dec $unique_cnt;
}

let $create_stmt = $create_stmt ) PARTITION BY $part_type(col1) PARTITIONS 100;
eval $create_stmt;

#Insert data
let $cnt = 100;
--disable_query_log
while ($cnt)
{
	eval INSERT INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt);
	dec $cnt;
}
--enable_query_log

# Show rows layout in all partitions
select count(partition_name) , sum(table_rows) from information_schema.partitions where table_name='t1' and table_schema='test' group by table_name;

# Show partition info
--source mysql_test/include/show_create_table_old_version.inc
show create table t1;
--disable_result_log
explain partitions select * from t1 where col1=100;
--enable_result_log

