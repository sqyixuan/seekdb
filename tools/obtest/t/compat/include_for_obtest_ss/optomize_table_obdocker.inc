connection tentsql_conn;
--disable_all_result_log
use testsql;
eval CREATE TABLE opt1 (col1 INT PRIMARY KEY, col2 VARCHAR(50));
eval CREATE TABLE opt2 (col1 INT PRIMARY KEY, col2 VARCHAR(50));
eval CREATE TABLE opt3 (col1 INT PRIMARY KEY, col2 VARCHAR(50));
ALTER TABLE opt1 set progressive_merge_num = 3;
ALTER TABLE opt2 set progressive_merge_num = 3;
ALTER TABLE opt3 set progressive_merge_num = 3;
let $cnt=1;
while($cnt <= 50)
{
  eval insert into  opt1 (col1,col2) values($cnt,"a_$cnt");
  eval insert into  opt2 (col1,col2) values($cnt,"a_$cnt");
  eval insert into  opt3 (col1,col2) values($cnt,"a_$cnt");
  inc $cnt;
}
#offline ddl
ALTER TABLE opt1 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt1 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt2 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt3 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt1 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt2 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt3 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt1 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt2 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt3 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;
ALTER TABLE opt1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;
ALTER TABLE opt1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;
ALTER TABLE opt1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;
ALTER TABLE opt1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;
ALTER TABLE opt1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt1 DROP COLUMN position1;
ALTER TABLE opt2 DROP COLUMN position1;
ALTER TABLE opt3 DROP COLUMN position1;



#online ddl
ALTER TABLE opt1 ADD birth_date DATE;
ALTER TABLE opt2 ADD birth_date DATE;
ALTER TABLE opt3 ADD birth_date DATE;
ALTER TABLE opt1 ADD birth_date1 DATE;
ALTER TABLE opt2 ADD birth_date1 DATE;
ALTER TABLE opt3 ADD birth_date1 DATE;
ALTER TABLE opt1 MODIFY birth_date1 DATE NULL;
ALTER TABLE opt2 MODIFY birth_date1 DATE NULL;
ALTER TABLE opt3 MODIFY birth_date1 DATE NULL;

OPTIMIZE TABLE opt1;
OPTIMIZE TABLE opt2;
OPTIMIZE TABLE opt3;
source t/compat/include_for_obtest_ss/optomize_table_20c_obdocker.inc;
--enable_all_result_log
