#owner: lb446709, qiyu.zd
#owner group: shenzhen
--echo # ----------------------------------------------------------------------
--echo # Test of sparse vector index
--echo # ----------------------------------------------------------------------

# insert 
insert into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}');
insert into test_sparse_vec_tbl values(2, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}');
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;

# insert on duplicate key
insert into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}') on duplicate key update c1 = false;
insert into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}') on duplicate key update c16 = null;
insert into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}') on duplicate key update c16 = '{1:3.4, 5:4.5}';
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;

# insert IGNORE into
--sorted_result
insert IGNORE into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,4,4,5], [1,2,4,4,5], [1,2,4,4,5], [1,2,4,4,5], '{1:123, 4:345}', '{1:123, 2:456}');
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;

# replace into
replace into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}');
replace into test_sparse_vec_tbl values(3, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{60:1.23, 80:4.56}');
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;
sleep 1;
select /*+read_consistency(weak) */ id, c11, c13, c16 from test_sparse_vec_tbl order by id;

# insert into select
--disable_warnings
drop table if exists test_sparse_vec_tbl_2;
--enable_warnings
create table test_sparse_vec_tbl_2 (
  id int, c1 bool, c2 int, c3 decimal, c4 float, c5 bit, 
  c6 date, c7 varchar(10), c8 text, c9 json, c10 GEOMETRY,
  c11 array(int), c12 vector(5), c13 vector(5), c14 vector(5), c15 map(int, int), c16 sparsevector, 
  vector index idx1(c16) with (distance=inner_product));

insert into test_sparse_vec_tbl_2 select * from test_sparse_vec_tbl;
select id, c1, c11, c13, c16 from test_sparse_vec_tbl_2 order by id;
delete from test_sparse_vec_tbl_2;

# 全量旁路导入
insert /*+ direct(true,100) enable_parallel_dml parallel(2) append*/ into test_sparse_vec_tbl_2 select * from test_sparse_vec_tbl;
--sorted_result
select id, c1, c11, c13, c16 from test_sparse_vec_tbl_2 order by id;
delete from test_sparse_vec_tbl_2;

# 增量旁路导入
insert /*+ direct(true,100,'inc') enable_parallel_dml parallel(2)*/  into test_sparse_vec_tbl_2 select * from test_sparse_vec_tbl;
--sorted_result
select id, c1, c11, c13, c16 from test_sparse_vec_tbl_2 order by id;
drop table if exists test_sparse_vec_tbl_2;

# major freeze
--disable_warnings
alter system major freeze;
--enable_warnings
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;

# update
update test_sparse_vec_tbl set c16 = '{1:1.5, 2:3, 5:6, 100:10}' where id = 1;
update /*+ ENABLE_PARALLEL_DML parallel(2) */ test_sparse_vec_tbl set c16 = '{1:1.5, 2:3, 5:6, 10:10}' where id = 1;
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;

# delete 
delete from test_sparse_vec_tbl where id < 4;
delete /*+ ENABLE_PARALLEL_DML parallel(2) */ from test_sparse_vec_tbl where id < 6;
select id, c1, c11, c13, c16 from test_sparse_vec_tbl;
delete from test_sparse_vec_tbl where c16 is null;
delete from test_sparse_vec_tbl where c16 is not null;
select id, c1, c11, c13, c16 from test_sparse_vec_tbl;

# truncate
insert into test_sparse_vec_tbl values(1, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', '{1:123, 2:456}');
insert into test_sparse_vec_tbl values(2, true, 1, 1, 1.3, 1, '2020-01-01', 'abc', 'abc', '{"a":1}', ST_GeomFromText('POINT(1 1)'), [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], [1,2,3,4,5], '{1:123, 4:345}', null);
select id, c1, c11, c13, c16 from test_sparse_vec_tbl order by id;
truncate table test_sparse_vec_tbl;

drop table if exists test_sparse_vec_tbl;
