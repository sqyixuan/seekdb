#owner: ryan.ly
#owner group: sql4
#description: greede_search建表

--disable_query_log
--disable_result_log

--source mysql_test/include/index_quick_major.inc

# tbl10 and tbl100 are only used to fill the tables used during testing
DROP TABLE IF EXISTS tbl10;
CREATE TABLE tbl10(
  k INT NOT NULL AUTO_INCREMENT,
  i INT,
  PRIMARY KEY(k)
);
INSERT INTO tbl10(i) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(0);
DROP TABLE IF EXISTS tbl100;
CREATE TABLE tbl100(
  k INT NOT NULL AUTO_INCREMENT,
  i INT,
  PRIMARY KEY(k)
);
INSERT INTO tbl100(i) SELECT x.i FROM tbl10 AS x, tbl10 AS y;

let $i= 1;
WHILE ($i < 10)
{
  --let $stmt= DROP TABLE IF EXISTS t10_$i
  --eval $stmt;
  --let $stmt= CREATE TABLE t10_$i (pk int PRIMARY KEY AUTO_INCREMENT, colidx int, col int, index (colidx))
  --eval $stmt;
  --let $stmt= INSERT INTO t10_$i (colidx, col) SELECT k, k FROM tbl10
  --eval $stmt;
  # Update index statistics
#  ANALYZE TABLE t10_$i;
  
  --let $stmt= DROP TABLE IF EXISTS t100_$i
  --eval $stmt;
  --let $stmt= CREATE TABLE t100_$i (pk int PRIMARY KEY AUTO_INCREMENT, colidx int, col int, index (colidx))
  --eval $stmt;
  --let $stmt= INSERT INTO t100_$i (colidx, col) SELECT a.k, a.k FROM tbl10 a JOIN tbl10 b
  --eval $stmt;
  # Update index statistics
#  ANALYZE TABLE t10_$i;

  inc $i;
}  

alter system set enable_sql_extension=true;
sleep 10;
analyze table tbl10 compute statistics for all columns size auto;
analyze table tbl100 compute statistics for all columns size auto;
analyze table t10_1 compute statistics for all columns size auto;
analyze table t10_2 compute statistics for all columns size auto;
analyze table t10_3 compute statistics for all columns size auto;
analyze table t10_4 compute statistics for all columns size auto;
analyze table t10_5 compute statistics for all columns size auto;
analyze table t10_6 compute statistics for all columns size auto;
analyze table t10_7 compute statistics for all columns size auto;
analyze table t10_8 compute statistics for all columns size auto;
analyze table t10_9 compute statistics for all columns size auto;
analyze table t100_1 compute statistics for all columns size auto;
analyze table t100_2 compute statistics for all columns size auto;
analyze table t100_3 compute statistics for all columns size auto;
analyze table t100_4 compute statistics for all columns size auto;
analyze table t100_5 compute statistics for all columns size auto;
analyze table t100_6 compute statistics for all columns size auto;
analyze table t100_7 compute statistics for all columns size auto;
analyze table t100_8 compute statistics for all columns size auto;
analyze table t100_9 compute statistics for all columns size auto;
alter system set enable_sql_extension=false;

--enable_result_log
--enable_query_log
