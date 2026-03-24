--disable_warnings
--error 0,942
drop table t1;
--enable_warnings
let $create_stmt = CREATE TABLE t1 (
	col1 INT primary key,
	col2 VARCHAR(20),
	col3 INT
;

while ($index_cnt)
{
	let $create_stmt = $create_stmt , ;
	dec $index_cnt;
}

while ($unique_cnt)
{
	let $create_stmt = $create_stmt , ;
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

# Show partition info
--source mysql_test/include/show_create_table_old_version.inc
show create table t1;
--disable_result_log
explain partitions select * from t1 where col1=100;
--enable_result_log

