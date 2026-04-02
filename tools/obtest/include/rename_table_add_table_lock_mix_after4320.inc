# connection conn_obtest_z1;
# mysql mode

connect (conn_obtest_z1_mix1,$ipz1,root@sys,,db1,$portz1);
connect (conn_obtest_z1_mix2,$ipz1,root@sys,,db1,$portz1);

connection conn_obtest_z1_mix1;
set autocommit=false;

start transaction;
insert into rename_table.t_mix(i, i_i, lob_t) values (1, 1, 'for test in mix');
connection conn_obtest_z1_mix2;
send rename table rename_table.t_mix to rename_table.t_mix_new;
connection conn_obtest_z1_mix1;
insert into rename_table.t_mix(i, i_i, lob_t) values (2, 2, 'for test in mix');
insert into rename_table.t_mix(i, i_i, lob_t) values (3, 3, 'for test in mix');
commit;
connection conn_obtest_z1_mix2;
reap;
connection conn_obtest_z1_mix1;
--error 1146
select * from rename_table.t_mix;
select * from rename_table.t_mix_new;
insert into rename_table.t_mix_new(i, i_i, lob_t) values (4, 4, 'for test in mix');
connection conn_obtest_z1_mix2;
send alter table rename_table.t_mix_new rename to rename_table.t_mix_new2;
connection conn_obtest_z1_mix1;
insert into rename_table.t_mix_new(i, i_i, lob_t) values (5, 5, 'for test in mix');
commit;
connection conn_obtest_z1_mix2;
reap;
connection conn_obtest_z1_mix1;
--error 1146
select * from rename_table.t_mix_new;
select * from rename_table.t_mix_new2;


# oracle mode
ob_connect(conn_oracle_obtest_mix1,ob1.proxy0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);
ob_connect(conn_oracle_obtest_mix2,ob1.proxy0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);
connection conn_oracle_obtest_mix1;
--disable_result_log
--error 0, 942
drop table t_mix;
--error 0, 942
drop table t_mix_new;
--error 0, 942
drop table t_mix_new2;
--enable_result_log
set autocommit=false;
create table t_mix (i int, i_i int, lob_t CLOB);
start transaction;
insert into t_mix(i, i_i, lob_t) values (1, 1, 'for test in mix');
connection conn_oracle_obtest_mix2;
send rename t_mix to t_mix_new;
connection conn_oracle_obtest_mix1;
insert into t_mix(i, i_i, lob_t) values (2, 2, 'for test in mix');
commit;
connection conn_oracle_obtest_mix2;
reap;
connection conn_oracle_obtest_mix1;
start transaction;
insert into t_mix_new(i, i_i, lob_t) values (3, 3, 'for test in mix');
select * from t_mix_new;
connection conn_oracle_obtest_mix2;
send alter table t_mix_new rename to t_mix_new2;
connection conn_oracle_obtest_mix1;
insert into t_mix_new(i, i_i, lob_t) values (4, 4, 'for test in mix');
commit;
connection conn_oracle_obtest_mix2;
reap;
connection conn_oracle_obtest_mix1;
insert into t_mix_new2(i, i_i, lob_t) values (5, 5, 'for test in mix');
select * from t_mix_new2;
