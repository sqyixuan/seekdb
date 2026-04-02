connection conn_mysql;
create database int2bigint;
create sequence int2bigint.s_mix cache 1000000 noorder;
create sequence int2bigint.s_finish cache 1000000 noorder;
create sequence int2bigint.s_finish_end cache 1000000 noorder;
create table int2bigint.t_foreign_depend (i_f_d int primary key);
create table int2bigint.t_mix (i_p int primary key, i_i int, i_g_d int, i_g_i_d int, i_f int, i_u int unsigned, i_c int, c char(20), c_g_d char(20), d decimal(10,2), d_i decimal(10,2), i_g int GENERATED ALWAYS AS (i_g_d+1) stored, i_g_i int GENERATED ALWAYS AS (i_g_i_d+1) stored, c_g char(40) GENERATED ALWAYS AS (concat(c_g_d,c_g_d)) virtual, foreign key (i_f) references int2bigint.t_foreign_depend(i_f_d), check (i_c<10000));
create table int2bigint.t_finish (i_p int primary key, i_i int, i_g_d int, i_g_i_d int, i_f int, i_u int unsigned, i_c int, c char(20), c_g_d char(20), d decimal(10,2), d_i decimal(10,2), i_g int GENERATED ALWAYS AS (i_g_d+1) stored, i_g_i int GENERATED ALWAYS AS (i_g_i_d+1) stored, c_g char(40) GENERATED ALWAYS AS (concat(c_g_d,c_g_d)) virtual, foreign key (i_f) references int2bigint.t_foreign_depend(i_f_d), check (i_c<10000));
 
insert into int2bigint.t_foreign_depend values (1);
create index idx_t_mix_i_i on int2bigint.t_mix(i_i);
create index idx_t_mix_d_i on int2bigint.t_mix(d_i);
create index idx_t_mix_i_g_i on int2bigint.t_mix(i_g_i);
create index idx_t_finish_i_i on int2bigint.t_finish(i_i);
create index idx_t_finish_d_i on int2bigint.t_finish(d_i);
create index idx_t_finish_i_g_i on int2bigint.t_finish(i_g_i);

insert into int2bigint.t_mix(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) 
                                values (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 2, 1, 2, 2, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 3, 1, 3, 3, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 4, 1, 4, 4, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 5, 1, 5, 5, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 6, 1, 6, 6, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 7, 1, 7, 7, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 8, 1, 8, 8, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 9, 1, 9, 9, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_mix.nextval, int2bigint.s_mix.nextval, 10, 1, 10, 10, 'feidu', 'feidu', 11.11, 11.11, 1);
insert into int2bigint.t_finish(i_p, i_i, i_g_d, i_f, i_u, i_c, c, c_g_d, d, d_i, i_g_i_d) 
                                values (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 1, 1, 1, 1, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 2, 1, 2, 2, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 3, 1, 3, 3, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 4, 1, 4, 4, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 5, 1, 5, 5, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 6, 1, 6, 6, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 7, 1, 7, 7, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 8, 1, 8, 8, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 9, 1, 9, 9, 'feidu', 'feidu', 11.11, 11.11, 1),
                                       (int2bigint.s_finish.nextval, int2bigint.s_finish.nextval, 10, 1, 10, 10, 'feidu', 'feidu', 11.11, 11.11, 1);

