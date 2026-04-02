# mysql mode
connect (conn_mysql_table_rename,$ip,root@sys,,*NO-ONE*,$port);
connect (conn_mysql_table_rename2,$ip,root@sys,,*NO-ONE*,$port);

connection conn_mysql_table_rename;
set autocommit=false;
start transaction;
insert into rename_table.t_finish(i, i_i, lob_t) values (2, 2, 'for test after upgrading');
connection conn_mysql_table_rename2;
send rename table rename_table.t_finish to rename_table.t_finish_new;
connection conn_mysql_table_rename;
insert into rename_table.t_finish(i, i_i, lob_t) values (2, 2, 'for test after upgrading');
commit;
connection conn_mysql_table_rename2;
reap;
connection conn_mysql_table_rename;
--error 1146
select * from rename_table.t_finish;
select * from rename_table.t_finish_new;

insert into rename_table.t_finish_new(i, i_i, lob_t) values (3, 3, 'for test after upgrading');
connection conn_mysql_table_rename2;
send alter table rename_table.t_finish_new rename to rename_table.t_finish_new2;
connection conn_mysql_table_rename;
insert into rename_table.t_finish_new(i, i_i, lob_t) values (4, 4, 'for test after upgrading');
commit;
connection conn_mysql_table_rename2;
reap;
connection conn_mysql_table_rename;
--error 1146
select * from rename_table.t_finish_new;
select * from rename_table.t_finish_new2;

# oracle mode
ob_connect(conn_oracle_table_rename,ob1.proxy0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);
ob_connect(conn_oracle_table_rename2,ob1.proxy0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);

connection conn_oracle_table_rename;
--disable_result_log
--error 0, 942
drop table t_finish;
--error 0, 942
drop table t_finish_new;
--error 0, 942
drop table t_finish_new2;
--enable_result_log
set autocommit=false;
create table t_finish (i int, i_i int, lob_t CLOB);
create index idx_t_finish_i_i on t_finish(i_i);
start transaction;
insert into t_finish values (111, 111, 'for test after upgrading');

connection conn_oracle_table_rename2;
send rename t_finish to t_finish_new;
connection conn_oracle_table_rename;
insert into t_finish(i, i_i, lob_t) values (2, 2, 'for test after upgrading');
commit;
connection conn_oracle_table_rename2;
reap;

connection conn_oracle_table_rename;
start transaction;
insert into t_finish_new(i, i_i, lob_t) values (3, 3, 'for test after upgrading');
select * from t_finish_new;
connection conn_oracle_table_rename2;
send alter table t_finish_new rename to t_finish_new2;
connection conn_oracle_table_rename;
insert into t_finish_new(i, i_i, lob_t) values (4, 4, 'for test after upgrading');
commit;
connection conn_oracle_table_rename2;
reap;
connection conn_oracle_table_rename;
insert into t_finish_new2(i, i_i, lob_t) values (5, 5, 'for test after upgrading');
select * from t_finish_new2;