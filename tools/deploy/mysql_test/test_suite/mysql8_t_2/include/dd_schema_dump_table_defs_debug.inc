--echo ########################################################################
--echo # Verify the DD_VERSION and the CREATE TABLE statements of the
--echo # DD tables.
--echo ########################################################################

--source mysql_test/test_suite/mysql8_t_2/include/have_debug.inc
SET debug = '+d,skip_dd_table_access_check';

--echo ########################################################################
--echo # The actual DD version stored on disk.
--echo ########################################################################
let $str = `SELECT SUBSTRING_INDEX(SUBSTRING(properties,
              LOCATE('DD_VERSION', properties), 30), ';', 1)
              AS DD_VERSION
              FROM mysql.dd_properties`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc

--echo ########################################################################
--echo # List the CREATE TABLE statements for the DD tables.
--echo # Mask the AUTO INCREMENT counter, which is not
--echo # deterministic when running mtr. Mask collations that
--echo # depend on the lower case table names setting.
--echo ########################################################################
replace_regex /`name` varchar\(64\) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/`name` varchar(64) <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.catalogs`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc

--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.character_sets`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.collations`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /`schema_name` varchar\(64\) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/`schema_name` varchar(64) <LCTN dependent collation> NOT NULL/
              /`table_name` varchar\(64\) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/`table_name` varchar(64) <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.column_statistics`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.column_type_elements`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.columns`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.dd_properties`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.events`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.foreign_key_column_usage`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /(`referenced_table_[a-z]+` varchar\(64\)) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/\1 <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.foreign_keys`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.index_column_usage`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.index_partitions`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.index_stats`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.indexes`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.innodb_ddl_log`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.innodb_dynamic_metadata`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.innodb_index_stats`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.innodb_table_stats`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.parameter_type_elements`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.parameters`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.resource_groups`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.routines`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /`name` varchar\(64\) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/`name` varchar(64) <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.schemata`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.st_spatial_reference_systems`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.table_partition_values`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.table_partitions`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.table_stats`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /`name` varchar\(64\) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/`name` varchar(64) <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.tables`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.tablespace_files`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.tablespaces`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
--replace_regex / AUTO_INCREMENT=[0-9]+//
let $str = `SHOW CREATE TABLE mysql.triggers`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /(`routine_catalog` varchar\(64\)) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/\1 <LCTN dependent collation> NOT NULL/
              /(`routine_schema` varchar\(64\)) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/\1 <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.view_routine_usage`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc
replace_regex /(`table_[a-z]+` varchar\(64\)) (CHARACTER SET utf8mb3 COLLATE utf8mb3_tolower_ci|CHARACTER SET utf8mb3 COLLATE utf8mb3_bin) NOT NULL/\1 <LCTN dependent collation> NOT NULL/
              / AUTO_INCREMENT=[0-9]+//;
let $str = `SHOW CREATE TABLE mysql.view_table_usage`;
--source mysql_test/test_suite/mysql8_t_2/include/write_line.inc

SET debug = '-d,skip_dd_table_access_check';
