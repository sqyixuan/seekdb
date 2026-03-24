SET NAMES UTF8;
set autocommit = on;
set ob_query_timeout=99900000000;
set ob_trx_timeout = 1000000000;  
 
--disable_query_log
--disable_warnings

drop table if exists myTable1;
CREATE TABLE myTable1(
    id INT(11) NOT NULL AUTO_INCREMENT,
    myTinyint TINYINT,
    mySmallint SMALLINT,
    myMediumint MEDIUMINT,
    myInt INT,
    myBigint BIGINT,
    myFloat FLOAT(8,2),
    myDouble DOUBLE(10,2),
    myDecimal DECIMAL(10,2),
    PRIMARY KEY (id)
);
INSERT INTO myTable1 (myTinyint, mySmallint, myMediumint, myInt, myBigint, myFloat, myDouble, myDecimal)
VALUES
(127, 32767, 8388607, 1000000, 9223372036854775807, 1.23, 3.45, 123.45),
(117, -32768, 885607, 2000000, 922337854775807, 2.34, 4.56, 234.56),
(1, 32167, 8388607, 3000000, -9223372036854775808, 3.45, 5.67, 345.67),
(0, 12345, -8388608, 4000000, 1.0+308, 4.56, 6.78, 456.78),
(-1, 0, 123456, 5000000, null, 5.67, 7.89, 567.89),
(null, -1, -1, 6000000, 1, 6.78, 8.90, 678.90),
(-128, null, 0, 7000000, 0, 7.89, 9.01, 789.01),
(99, 1, 1, 8000000, -1, 8.90, 1.23, 890.12),
(9, 99, null, 9000000, 900000000, 9.01, 2.34, 901.23),
(-123, -111, 100000, 10000000, -9223372036854775808, 1.23, 3.45, 12.34);


drop table if exists  myTable2;
CREATE TABLE myTable2 (
    id INT(11) NOT NULL AUTO_INCREMENT,
    myDate DATE,
    myTime TIME,
    myYear YEAR(4),
    myDatetime DATETIME,
    myTimestamp TIMESTAMP,
    PRIMARY KEY (id)
);
INSERT INTO myTable2 (myDate, myTime, myYear, myDatetime, myTimestamp)
VALUES
('0000-01-01', '12:34:56', '2037', '0000-01-01 12:34:56', '0000-01-01 12:34:56'),
('0000-02-02', '23:45:00', '2021', '0000-02-02 23:45:00', '0000-02-02 23:45:00'),
('2023-03-03', '06:15:30', '2020', '2023-03-03 06:15:30', '2023-03-03 06:15:30'),
('2023-05-04', '17:30:45', '1969', '2023-05-04 17:30:45', '2023-05-04 17:30:45'),
('2023-06-05', '10:00:00', '1997', '2023-06-05 10:00:00', '2023-06-05 10:00:00'),
('2023-07-06', '14:20:30', '1996', '2023-07-06 14:20:30', '2023-07-06 14:20:30'),
('2023-08-07', '08:45:15', '1969', '2023-08-07 08:45:15', '2023-08-07 08:45:15'),
('2023-09-08', '19:10:00', '2023', '2023-09-08 19:10:00', '2023-09-08 19:10:00'),
('2023-10-09', '02:30:10', '2023', '2023-10-09 02:30:10', '2023-10-09 02:30:10'),
('9999-12-10', '21:50:30', '2038', '9999-12-10 21:50:30', '9999-12-10 21:50:30');

drop table if exists  myTable3;
CREATE TABLE myTable3 (
   id INT PRIMARY KEY,
   char_col CHAR(50),
   varchar_col VARCHAR(50),
   tinyblob_col TINYBLOB,
   tinytext_col TINYTEXT,
   blob_col BLOB,
   text_col TEXT,
   mediumtext_col MEDIUMTEXT,
   longblob_col LONGBLOB,
   longtext_col LONGTEXT
);
INSERT INTO myTable3 (id, char_col, varchar_col, tinyblob_col, tinytext_col, blob_col, text_col, mediumtext_col, longblob_col, longtext_col)
VALUES
(1, '中文1Abc123', 'vArchAr1Abc中文', 'tinyblob1中文Abc', 'tinytext1Abc中文', 'blob1Abc', 'text1中文Abc123', 'mediumtext1中文Abc123', 'longblob1中文Abc', 'longtext1中文Abc123'),
(5, '中文5Abc123', 'vArchAr2Abc中文', 'tinyblob2中文Abc', 'tinytext2Abc中文', 'blob2Abc', 'text2中文Abc123', 'mediumtext2中文Abc123', 'longblob2中文Abc', 'longtext2中文Abc123'),
(7, '中文7Abc123', 'vArchAr3Abc中文', 'tinyblob3中文Abc', 'tinytext3Abc中文', 'blob3Abc', 'text3中文Abc123', 'mediumtext3中文Abc123', 'longblob3中文Abc', 'longtext3中文Abc123'),
(9, '中文9Abc123', 'vArchAr9Abc中文', 'tinyblob9中文Abc', 'tinytext9Abc中文', 'blob9Abc', 'text9中文Abc123', 'mediumtext9中文Abc123', 'longblob9中文Abc', 'longtext9中文Abc123'),
(10, '中文10Abc123', 'vArchAr10Abc中文', 'tinyblob10中文Abc', 'tinytext10Abc中文', 'blob10中Abc', 'text10中文Abc123', 'mediumtext10中文Abc123', 'longblob10中文Abc', 'longtext10中文Abc123');


drop table if exists  t1;
create table t1(c1 int PRIMARY KEY,c2 varchar(20));
insert into t1 values(1,'中文测试ing');
insert into t1 values(5,'中文测试5ing');
insert into t1 values(7,'中文测试7ing');
insert into t1 values(10,'中文测试10ing');

drop table if exists example ;
CREATE TABLE example (
id int(11) NOT NULL AUTO_INCREMENT,
name varchar(50) NOT NULL,
colors set('red','green','blue','yellow') NOT NULL,
fruit enum('apple','orange','banana','grape','mango') NOT NULL,
PRIMARY KEY (id)
);

INSERT INTO example (name, colors, fruit) VALUES
('Alice', 'red,green', 'apple'),
('Bob', 'blue', 'mango'),
('Charlie', 'green,yellow', 'banana'),
('David', 'red,blue,yellow', 'orange'),
('Eva', 'red', 'apple'),
('Frank', 'green,blue', 'banana'),
('Grace', 'red,green,yellow', 'grape'),
('Henry', 'blue,yellow', 'mango'),
('Ivy', 'red,green,blue,yellow', 'orange'),
('John', 'green', 'banana');

drop table if exists ljh_test1 ;
CREATE TABLE ljh_test1( 
  c1 char(5));
insert into ljh_test1 values('');
insert into ljh_test1 values(null);
insert into ljh_test1 values('1');
insert into ljh_test1 values('-1.1');
insert into ljh_test1 values('a');

drop table if exists ljh_test2 ;
CREATE TABLE ljh_test2( 
  c2 varchar(5));
insert into ljh_test2 values('');
insert into ljh_test2 values(null);
insert into ljh_test2 values('1');
insert into ljh_test2 values('-1.1');
insert into ljh_test2 values('a');

drop table if exists ljh_test3 ;
CREATE TABLE ljh_test3( 
  c3 tinyblob);
insert into ljh_test3 values('');
insert into ljh_test3 values(null);
insert into ljh_test3 values('1');
insert into ljh_test3 values('-1.1');
insert into ljh_test3 values('a');


drop table if exists ljh_test4 ;
CREATE TABLE ljh_test4( 
  c4 tinyblob);
insert into ljh_test4 values('');
insert into ljh_test4 values(null);
insert into ljh_test4 values('1');
insert into ljh_test4 values('-1.1');
insert into ljh_test4 values('a');
insert into ljh_test4 values('David!');

drop table if exists ljh_test5 ;
CREATE TABLE ljh_test5( 
  c5 blob);
insert into ljh_test5 values('');
insert into ljh_test5 values(null);
insert into ljh_test5 values('1');
insert into ljh_test5 values('-1.1');
insert into ljh_test5 values('a');
insert into ljh_test5 values('David!');

drop table if exists ljh_test6 ;
CREATE TABLE ljh_test6( 
  c6 text);
insert into ljh_test6 values('');
insert into ljh_test6 values(null);
insert into ljh_test6 values('1');
insert into ljh_test6 values('-1.1');
insert into ljh_test6 values('a');
insert into ljh_test6 values('David!');

drop table if exists ljh_test7 ;
CREATE TABLE ljh_test7( 
  c7 mediumtext);
insert into ljh_test7 values('');
insert into ljh_test7 values(null);
insert into ljh_test7 values('1');
insert into ljh_test7 values('-1.1');
insert into ljh_test7 values('a');
insert into ljh_test7 values('David!');

drop table if exists ljh_test8 ;
CREATE TABLE ljh_test8( 
  c8 longblob);
insert into ljh_test8 values('');
insert into ljh_test8 values(null);
insert into ljh_test8 values('1');
insert into ljh_test8 values('-1.1');
insert into ljh_test8 values('a');
insert into ljh_test8 values('David!');

drop table if exists ljh_test9 ;
CREATE TABLE ljh_test9( 
  c9 longtext);
insert into ljh_test9 values('');
insert into ljh_test9 values(null);
insert into ljh_test9 values('1');
insert into ljh_test9 values('-1.1');
insert into ljh_test9 values('a');
insert into ljh_test9 values('David!');

drop table if exists cast_table1 ;
create table cast_table1(a float) ;
insert into cast_table1 values(-5656.565);


drop table if exists cast_table2 ;
create table cast_table2(a char(10));
insert into cast_table2 values('kebi'),('kuli'),('0'),(null);

--echo ##生僻字验证

drop table if exists my_table;
CREATE TABLE my_table(c1 varchar(20), c2 varchar(2000), c3 varchar(2000));
INSERT INTO my_table VALUES ('09','䪎䪎䪎','蟰蟰蟰蟰蟰');
INSERT INTO my_table VALUES ('999','黒 匼 ','黒 匼 ');
INSERT INTO my_table VALUES ('9999','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
INSERT INTO my_table VALUES ('11111','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

--enable_query_log
--enable_warnings
 
SELECT 1;