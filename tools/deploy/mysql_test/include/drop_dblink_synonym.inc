# 删除link表同义词

--disable_query_log
--disable_result_log

if ($DBLINK != '') {

# 在本地租户构造drop_synonym语句并执行

connection normal_conn;

--disable_warnings
--error 0, 942
drop table dblink_syno_stmts;
--enable_warnings

eval create table dblink_syno_stmts as
  select 'DROP synonym ' || owner || '.' || synonym_name stmt_str, rownum as stmt_no from all_synonyms;

let $idx = 0;
let $synonym_count = query_get_value(select count(*) as cnt from dblink_syno_stmts, CNT, 1);
while ($idx < $synonym_count) {
  inc $idx;
  let $synonym_stmt = query_get_value(select stmt_str from dblink_syno_stmts where stmt_no = $idx, STMT_STR, 1);
  eval $synonym_stmt;
}

drop table dblink_syno_stmts;

}

--enable_result_log
--enable_query_log

