# 创建link表同义词
# 需要指定link租户连接信息和dblink名
# 本地租户使用默认连接

# 使用示例
# let $TENANT = XXX
# let $USER = XXX
# let $PWD = XXX
# let $DBLINK = XXX
# --source mysql_test/include/create_dblink_synonym.inc

--source mysql_test/include/drop_dblink_synonym.inc

--disable_query_log
--disable_result_log

if ($DBLINK != '') {

# 在link租户构造create_synonym语句并在本地租户默认连接执行

connection dblink_conn;

--disable_warnings
--error 0, 942
drop table synonym_stmts;
--enable_warnings

# OB will see the tmp table name from all_tables in create table as query, which will lead to unexpected behavior,
# so we break the create table as select into 2 stmts
create table synonym_stmts(stmt_str varchar(500), stmt_no number);
eval insert into synonym_stmts select 'CREATE OR REPLACE synonym ' || table_name || ' for ' || owner || '.' || table_name || '@' || '$DBLINK' as stmt_str, rownum as stmt_no from all_tables where OWNER = sys_context('USERENV', 'CURRENT_SCHEMA');

let $idx = 0;
let $synonym_count = query_get_value(select count(*) as cnt from synonym_stmts, CNT, 1);
while ($idx < $synonym_count) {
  inc $idx;
  connection dblink_conn;
  let $synonym_stmt = query_get_value(select stmt_str from synonym_stmts where stmt_no = $idx, STMT_STR, 1);
  connection normal_conn;
  eval $synonym_stmt;
}

connection dblink_conn;

drop table synonym_stmts;

}

--enable_result_log
--enable_query_log

