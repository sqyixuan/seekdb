connection conn_mysql;
create table int2bigint.t_finish_end (i_p int primary key, i_i int, i_g_d int, i_g_i_d int, i_f int, i_u int unsigned, i_c int, c char(20), c_g_d char(20), d decimal(10,2), d_i decimal(10,2), i_g int GENERATED ALWAYS AS (i_g_d+1) stored, i_g_i int GENERATED ALWAYS AS (i_g_i_d+1) stored, c_g char(40) GENERATED ALWAYS AS (concat(c_g_d,c_g_d)) virtual, foreign key (i_f) references int2bigint.t_foreign_depend(i_f_d), check (i_c<10000));
create index idx_t_finish_end_i_i on int2bigint.t_finish_end(i_i);
create index idx_t_finish_end_d_i on int2bigint.t_finish_end(d_i);
create index idx_t_finish_end_i_g_i on int2bigint.t_finish_end(i_g_i);
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) 
                                values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 2, 1, 2, 2, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 3, 1, 3, 3, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 4, 1, 4, 4, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 5, 1, 5, 5, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 6, 1, 6, 6, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 7, 1, 7, 7, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 8, 1, 8, 8, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 9, 1, 9, 9, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 10, 1, 10, 10, 'feidu', 'feidu', 11.11, 11.11, 1);
 
#online
let $t_finish_end_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish_end change column i_p i_p bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column i_i i_i bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column i_g_d i_g_d bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column i_c i_c bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column c c char(40);
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column d d decimal(18,2);
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column d_i d_i decimal(18,2);
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column i_g i_g bigint GENERATED ALWAYS AS (i_g_d+1) stored;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish_end change column c_g c_g char(80) GENERATED ALWAYS AS (concat(c_g_d, c_g_d)) virtual;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_end_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_end_id_alterred == $t_finish_end_id_origin)
{
--echo online
}
if ($t_finish_end_id_alterred != $t_finish_end_id_origin)
{
--echo offline
}
let $t_finish_end_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish_end change column i_u i_u bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_end_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_end_id_alterred == $t_finish_end_id_origin)
{
--echo online
}
if ($t_finish_end_id_alterred != $t_finish_end_id_origin)
{
--echo offline
}
# char with generated column depended offline
let $t_finish_end_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish_end change column c_g_d c_g_d char(40);
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_end_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_end_id_alterred == $t_finish_end_id_origin)
{
--echo online
}
if ($t_finish_end_id_alterred != $t_finish_end_id_origin)
{
--echo offline
}
# offline
let $t_finish_end_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish_end change column i_g_i_d i_g_i_d bigint;
let $t_finish_end_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_end_id_alterred == $t_finish_end_id_origin)
{
--echo online
}
if ($t_finish_end_id_alterred != $t_finish_end_id_origin)
{
--echo offline
}
--error 5320
alter table int2bigint.t_finish_end change column i_f i_f bigint;
insert into int2bigint.t_finish_end(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish_end.nextval, int2bigint.s_finish_end.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
 
#online 
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column i_p i_p bigint;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column i_i i_i bigint;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column i_g_d i_g_d bigint;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column i_c i_c bigint;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column c c char(40);
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column i_g i_g bigint GENERATED ALWAYS AS (i_g_d+1) stored;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
alter table int2bigint.t_finish change column c_g c_g char(80) GENERATED ALWAYS AS (concat(c_g_d, c_g_d)) virtual;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column i_u i_u bigint;
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish_end' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
# decimal -> decimalint offline 
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column d d decimal(18,2);
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
# decimal -> decimalint offline 
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column d_i d_i decimal(18,2);
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
# char with generated column depended offline
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column c_g_d c_g_d char(40);
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1);
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
# offline
let $t_finish_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
alter table int2bigint.t_finish change column i_g_i_d i_g_i_d bigint;
let $t_finish_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_finish' and database_id = (select database_id from oceanbase.__all_database where database_name = 'int2bigint'), table_id, 1);
if ($t_finish_id_alterred == $t_finish_id_origin)
{
--echo online
}
if ($t_finish_id_alterred != $t_finish_id_origin)
{
--echo offline
}
--error 5320
alter table int2bigint.t_finish change column i_f i_f bigint;

connection conn_oracle;
create table t_oracle_int_float_end (a NUMBER(38) primary key);
insert into t_oracle_int_float_end(a) values (1);

connection conn_mysql;
let $t_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_oracle_int_float_end', table_id, 1);
connection conn_oracle;
alter table t_oracle_int_float_end modify a float;
insert into t_oracle_int_float_end(a) values (1.2);
connection conn_mysql;
let $t_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_oracle_int_float_end', table_id, 1);
if ($t_id_alterred == $t_id_origin)
{
--echo online
}
if ($t_id_alterred != $t_id_origin)
{
--echo offline
}

connection conn_mysql;
let $t_id_origin = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_oracle_int_float', table_id, 1);
connection conn_oracle;
alter table t_oracle_int_float modify a float;
insert into t_oracle_int_float(a) values (1.2);
connection conn_mysql;
let $t_id_alterred = query_get_value(select table_id from oceanbase.__all_table where table_name = 't_oracle_int_float', table_id, 1);
if ($t_id_alterred == $t_id_origin)
{
--echo online
}
if ($t_id_alterred != $t_id_origin)
{
--echo offline
}

