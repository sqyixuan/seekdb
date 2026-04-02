# table for column name.
--disable_warnings
drop table if exists t_col;
--enable_warnings
create table t_col(id int auto_increment primary key, name varchar(100), not_neg bool, zero_scale bool);

## arith test.
--disable_warnings
drop table if exists t_op;
--enable_warnings
--disable_warnings
drop table if exists t_sql;
--enable_warnings
create table t_op(id int auto_increment primary key, name varchar(16));
create table t_sql(id int auto_increment primary key, c1 varchar(16), c2 varchar(16), op varchar(16), cond varchar(256) default '',
                    not_neg1 bool, zero_scale1 bool, not_neg2 bool, zero_scale2 bool);
insert into t_op(name)
  values ('+'), ('-'), ('*'), ('/'), ('div'), ('mod');
insert into t_sql(c1, c2, op, not_neg1, zero_scale1, not_neg2, zero_scale2)
  select t_col1.name as c1, t_col2.name as c2, t_op.name as op,
         t_col1.not_neg as not_neg1, t_col1.zero_scale as zero_scale1, t_col2.not_neg as not_neg2, t_col2.zero_scale as zero_scale2
  from t_col as t_col1, t_col as t_col2, t_op
  order by t_col1.id, t_col2.id, t_op.id;
update t_sql
  set cond = concat(' and (c1 + 0.0) ', op ,' (c2 + 0.0) >= 0')
  where op in ('+', '-', '*') and ((not_neg1 or not_neg2) and (zero_scale1 and zero_scale2));
update t_sql
  set cond = ' and (c1 + 0.0) / (c2 + 0.0) >= 0'
  where op = 'div' and ((not_neg1 and zero_scale1) or (not_neg2 and zero_scale2));
select concat('select c1, c2, ', group_concat(concat('c1 ', op, ' c2') order by t_op.id separator ', '),
             ' from (select t1.', c1, ' as c1, t2.', c2, ' as c2 from t t1, t t2) as tmp',
             ' where c1 is not null and c2 is not null', cond,
             ' order by c1, c2;') as stmt
  from t_sql, t_col t_col1, t_col t_col2, t_op
  where c1 = t_col1.name and c2 = t_col2.name and op = t_op.name
  group by c1, c2, cond
  order by t_col1.id, t_col2.id, t_op.id;
