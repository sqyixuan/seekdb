let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# online
alter table int2bigint.t_mix change column c c char(40);
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# online
alter table int2bigint.t_mix change column d d decimal(18,2);
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# online
alter table int2bigint.t_mix change column d_i d_i decimal(18,2);
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# online
alter table int2bigint.t_mix change column c_g c_g char(80) GENERATED ALWAYS AS (concat(c_g_d, c_g_d)) virtual;
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
 
# 大约13分钟到这里
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_mix change column i_g_d i_g_d bigint;
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# online ddl
alter table int2bigint.t_mix change column i_g i_g bigint GENERATED ALWAYS AS (i_g_d+1) stored;
insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 11, 1, 11, 11, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# unsinged int -> smallint unsigned : integer column change : offline ddl
--error 1235
alter table int2bigint.t_mix change column i_u i_u smallint unsigned;
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# index column : integer column change : offline ddl
alter table int2bigint.t_mix change column i_i i_i bigint;
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# has generated column depended : char column : offline ddl
--error 1235
alter table int2bigint.t_mix change column c_g_d c_g_d char(40); 
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
# primary key column : integer column change : offline ddl
alter table int2bigint.t_mix change column i_p i_p bigint; 
 
let $t_mix_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_mix change column i_c i_c bigint;
let $t_mix_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_mix' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_mix_id_alterred == $t_mix_id_origin)
{
--echo online
}
if ($t_mix_id_alterred != $t_mix_id_origin)
{
--echo offline
}

alter table int2bigint.t_mix change column i_g_i_d i_g_i_d bigint;
--error 5320
alter table int2bigint.t_mix change column i_f i_f bigint;

ob_connect(conn_oracle_obtest,ob1.proxy0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);
ob_connect(conn_oracle_obtest_z3,ob1.z3.obs0,OBTEST@oracle,obtest,OBTEST,,USE_OB_CONNECTOR);
--error 600
alter table t_oracle_int_float modify a float;
