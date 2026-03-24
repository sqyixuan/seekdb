connection tentsql_conn;
use testsql;
--disable_all_result_log
select * from tableddl;
select * from ordr;
ALTER TABLE ordr  ADD cafter_after  INT;
ALTER TABLE ordr  ADD cfirst_after  INT FIRST;
ALTER TABLE ordr ADD id_after INT;
ALTER TABLE ordr  MODIFY cafter1 DECIMAL(15, 1);
ALTER TABLE ordr  DROP COLUMN full_name;
ALTER TABLE ordr ADD COLUMN full_name VARCHAR(255) AS (CONCAT(c1, ' ', c2)) STORED;



#online ddl
ALTER TABLE tableddl ADD birth_dateafter DATE;
ALTER TABLE tableddl ADD COLUMN full_nameafter VARCHAR(255) AS (CONCAT(name, ' ', hire_date)) VIRTUAL;
ALTER TABLE tableddl MODIFY date_of_birth DATE NULL;
ALTER TABLE tableddl ALTER COLUMN is_active SET DEFAULT '1';
ALTER TABLE tableddl ALTER COLUMN is_active DROP DEFAULT;
ALTER TABLE tableddl AUTO_INCREMENT = 2000;
ALTER TABLE tableddl CHANGE COLUMN date_of_birth birthday_date DATE;
ALTER TABLE tableddl MODIFY COLUMN salary DECIMAL(15, 1);
ALTER TABLE tableddl add  temp_column_after int;
CREATE INDEX idx_last_name_after ON tableddl (name);
DROP INDEX idx_last_name_after ON tableddl;
RENAME TABLE tableddl TO staff;
ALTER TABLE staff ROW_FORMAT = DYNAMIC;
ALTER TABLE staff COMPRESSION = 'zstd_1.0';
OPTIMIZE TABLE staff;
RENAME TABLE staff to tableddl;

select * from tableddl;
select * from ordr;
--enable_all_result_log
