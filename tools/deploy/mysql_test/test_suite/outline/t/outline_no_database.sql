alter system flush plan cache global;
drop table if exists test.t1;
create table test.t1(c1 int, c2 int, c3 int);
create index test.idx_c3 on test.t1(c3);

select * from test.t1;
select sql_id, query_sql, outline_id from oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT where query_sql like 'select * from test.t1';

#echo create outline using sql_id
create outline otl_no_database on '05CD29E113763D1C9FA4D01028685406' using hint /*+ index(test.t1 idx_c3)*/;
select outline_id, database_id, database_name from oceanbase.DBA_OB_OUTLINES where outline_name like 'otl_no_database';

#echo use outline
select * from test.t1;
select sql_id, query_sql, outline_id from oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT where query_sql like 'select * from test.t1';

#echo drop outline
drop outline otl_no_database;
select outline_id, database_id, database_name from oceanbase.DBA_OB_OUTLINES where outline_name like 'otl_no_database';

select sql_id, query_sql, outline_id from oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT where query_sql like 'select * from test.t1';
#echo create outline using sql text
create outline otl_no_database on select /*+ index(test.t1 idx_c3)*/* from test.t1;
select outline_id, database_id, database_name from oceanbase.DBA_OB_OUTLINES where outline_name like 'otl_no_database';
#echo use outline
select * from test.t1;
select sql_id, query_sql, outline_id from oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT where query_sql like 'select * from test.t1';

drop outline otl_no_database;
drop table if exists test.t1;
