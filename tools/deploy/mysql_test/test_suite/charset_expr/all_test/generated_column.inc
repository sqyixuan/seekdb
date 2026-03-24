--error 0,942
drop table t_charset_gc;

create table t_charset_gc(id int, name varchar2(32), gc as (lower(name)));
create index i1_t_charset_gc on t_charset_gc (upper(name));
desc t_charset_gc;
insert into t_charset_gc(id, name) values (1,'类茫：DGLAN1KDG1xxxxSDF00.uu');
select id, name, gc, lengthb(name) name_len from t_charset_gc;

drop table t_charset_gc;
