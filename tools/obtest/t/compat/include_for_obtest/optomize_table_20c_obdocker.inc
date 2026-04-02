connection tentsql_conn;
use testsql;
eval CREATE TABLE opt_1 (col1 INT PRIMARY KEY,col2 VARCHAR(50), col3 VARCHAR(50), col4 VARCHAR(50), col5 VARCHAR(50), col6 VARCHAR(50), col7 VARCHAR(50), col8 VARCHAR(50), col9 VARCHAR(50), col10 VARCHAR(50), col11 VARCHAR(50), col12 VARCHAR(50), col13 VARCHAR(50), col14 VARCHAR(50), col15 VARCHAR(50), col16 VARCHAR(50), col17 VARCHAR(50), col18 VARCHAR(50), col19 VARCHAR(50), col20 VARCHAR(50), col21 VARCHAR(50), col22 VARCHAR(50), col23 VARCHAR(50), col24 VARCHAR(50), col25 VARCHAR(50)) WITH COLUMN GROUP(all columns, each column);
eval CREATE TABLE opt_2 (col1 INT PRIMARY KEY, col2 VARCHAR(50),col3 VARCHAR(50), col4 VARCHAR(50), col5 VARCHAR(50), col6 VARCHAR(50), col7 VARCHAR(50), col8 VARCHAR(50), col9 VARCHAR(50), col10 VARCHAR(50), col11 VARCHAR(50), col12 VARCHAR(50), col13 VARCHAR(50), col14 VARCHAR(50), col15 VARCHAR(50), col16 VARCHAR(50), col17 VARCHAR(50), col18 VARCHAR(50), col19 VARCHAR(50), col20 VARCHAR(50), col21 VARCHAR(50), col22 VARCHAR(50), col23 VARCHAR(50), col24 VARCHAR(50), col25 VARCHAR(50)) WITH COLUMN GROUP(each column);
eval CREATE TABLE opt_3 (col1 INT PRIMARY KEY, col2 VARCHAR(50),col3 VARCHAR(50), col4 VARCHAR(50), col5 VARCHAR(50), col6 VARCHAR(50), col7 VARCHAR(50), col8 VARCHAR(50), col9 VARCHAR(50), col10 VARCHAR(50), col11 VARCHAR(50), col12 VARCHAR(50), col13 VARCHAR(50), col14 VARCHAR(50), col15 VARCHAR(50), col16 VARCHAR(50), col17 VARCHAR(50), col18 VARCHAR(50), col19 VARCHAR(50), col20 VARCHAR(50), col21 VARCHAR(50), col22 VARCHAR(50), col23 VARCHAR(50), col24 VARCHAR(50), col25 VARCHAR(50));
ALTER TABLE opt_1 set progressive_merge_num = 3;
ALTER TABLE opt_2 set progressive_merge_num = 3;
ALTER TABLE opt_3 set progressive_merge_num = 3;
--disable_all_result_log
let $cnt=1;
while($cnt <= 500)
{
  eval insert into  opt_1 (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25) values($cnt,"a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt");
  eval insert into  opt_2 (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25) values($cnt,"a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt");
  eval insert into  opt_3 (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25) values($cnt,"a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt","a_$cnt");
  inc $cnt;
}
--enable_all_result_log
#offline ddl
ALTER TABLE opt_1 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt_2 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt_3 CHANGE COLUMN position position1 VARCHAR(50) AFTER col2;
ALTER TABLE opt_1 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt_2 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt_3 MODIFY col1 INT AUTO_INCREMENT;
ALTER TABLE opt_1 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt_2 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt_3 ADD position2 VARCHAR(50) AFTER col2;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;
ALTER TABLE opt_1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;
ALTER TABLE opt_1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;
ALTER TABLE opt_1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;
ALTER TABLE opt_1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;
ALTER TABLE opt_1 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_2 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_3 ADD position1 VARCHAR(50) AFTER col1;
ALTER TABLE opt_1 DROP COLUMN position1;
ALTER TABLE opt_2 DROP COLUMN position1;
ALTER TABLE opt_3 DROP COLUMN position1;



#online ddl
ALTER TABLE opt_1 ADD birth_date DATE;
ALTER TABLE opt_2 ADD birth_date DATE;
ALTER TABLE opt_3 ADD birth_date DATE;
ALTER TABLE opt_1 ADD birth_date1 DATE;
ALTER TABLE opt_2 ADD birth_date1 DATE;
ALTER TABLE opt_3 ADD birth_date1 DATE;
ALTER TABLE opt_1 MODIFY birth_date1 DATE NULL;
ALTER TABLE opt_2 MODIFY birth_date1 DATE NULL;
ALTER TABLE opt_3 MODIFY birth_date1 DATE NULL;

OPTIMIZE TABLE opt_1;
OPTIMIZE TABLE opt_2;
OPTIMIZE TABLE opt_3;
