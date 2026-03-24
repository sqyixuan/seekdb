connection tentsql_conn;
--disable_all_result_log
use mutitable;
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl2_1 (id, name) VALUES ($cnt, 'Alice');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO sale_1 (date_col, sales_col, name_col) VALUES (NOW(), $cnt, '$cnt');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO sale_1 (date_col, sales_col, name_col) VALUES (NOW(), $cnt, '$cnt');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl1_1 (id, name, date) VALUES ($cnt, '$cnt', NOW());
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl1_l_1(col1,col2) values ($cnt,'test_$cnt');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (1,'00');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (2,'01');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (3,'02');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (4,'03');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (5,'04');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (6,'05');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (7,'06');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (8,'07');
 eval INSERT INTO tbl1_lc_1(id,partition_id) values (9,'08');
 inc $cnt;
}
let $cnt=2000;
while($cnt <= 2600)
{
 eval INSERT INTO tbl1_h_1 (col1,col2) VALUES ($cnt, '$cnt');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl1_k_1(id, gmt_create, info) VALUES ($cnt,NOW(),'$cnt');
 inc $cnt;
}
let $cnt=0;
while($cnt < 300)
{
 eval INSERT INTO tbl_rr_1(col1,col2,col3)  VALUES ($cnt,$cnt,$cnt);
 inc $cnt;
}
let $cnt=0;
while($cnt < 300)
{
 eval INSERT INTO t2_m_rcr_1 (col1,col2,col3)  VALUES ($cnt,$cnt,$cnt);
 inc $cnt;
}
let $cnt=0;
while($cnt < 300)
{
 eval INSERT INTO t_m_rclc_1(col1,col2) VALUES ($cnt,1);
 eval INSERT INTO t_m_rclc_1(col1,col2) VALUES ($cnt,3);
 eval INSERT INTO t_m_rclc_1(col1,col2) VALUES ($cnt,4);
 eval INSERT INTO t_m_rclc_1(col1,col2) VALUES ($cnt,6);
 eval INSERT INTO t_m_rclc_1(col1,col2) VALUES ($cnt,7);
 inc $cnt;
}
let $cnt=0;
while($cnt < 300)
{
 eval INSERT INTO t2_m_rch_1(col1,col2) VALUES ($cnt,$cnt);
 inc $cnt;
}
let $cnt=1;
while($cnt <= 500)
{
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,1,$cnt);
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,3,$cnt);
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,4,$cnt); 
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,6,$cnt);
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,7,$cnt);
 eval INSERT INTO tbl2_m_klc_1(col1,col2,col3)  VALUES ($cnt,9,$cnt);
 inc $cnt;
}
let $cnt=0;
while($cnt <= 50)
{
 eval INSERT INTO t2_f_rr_1 (col1, col2) VALUES ($cnt, '2021-02-15 10:30:45'),($cnt, '2021-05-20 14:23:45'),($cnt, '2021-08-10 08:15:30'),($cnt, '2021-11-05 16:45:00'),($cnt, '2021-12-25 20:00:00');
 inc $cnt;
}
let $cnt=0;
while($cnt <= 50)
{
 eval INSERT INTO t2_f_hk_1(col1,col2)  VALUES ($cnt,$cnt);
 inc $cnt;
}
let $cnt=0;
while($cnt <= 50)
{
 eval INSERT INTO t_1 (c1, c2, c3, c4) VALUES ('short', 'medium length text', REPEAT('a', 30000), REPEAT('b', 10000000)),('example', 'another medium text', REPEAT('c', 30000), REPEAT('d', 10000000)),('last', 'final medium text', REPEAT('e', 30000), REPEAT('f', 10000000));
 inc $cnt;
}
let $cnt=0;
while($cnt <= 500)
{
 eval INSERT INTO tbl1_g_1 (g) VALUES (ST_GeomFromText('POINT(10 20)', 0));
 eval INSERT INTO tbl1_g_1 (g) VALUES (ST_GeomFromText('LINESTRING(10 10, 20 20, 30 40)', 0));
 eval INSERT INTO tbl1_g_1 (g) VALUES (ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 0));
 eval INSERT INTO tbl1_g_1 (g) VALUES (ST_GeomFromText('MULTIPOINT((10 20), (30 40), (50 60))', 0));
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval INSERT INTO tbl1txt_1 (id, docs, docs1) VALUES  ($cnt, JSON_OBJECT('name', 'name_$cnt', 'age', 30), JSON_ARRAY('red', 'green', 'blue'));
 inc $cnt;
}
let $cnt=60;
--error 1451,0
delete from mutidata1_1;
while($cnt <= 99)
{
 eval INSERT INTO mutidata1_1 (c1, c2, c3, c4, c5, c6, c7, col1) VALUES  (1234.$cnt, 32767, 9876543210, b'1010', 8388607, '2023-10-01 12:34:56.123456', '2023-10-01 12:34:56.12345', $cnt);
 inc $cnt;
}
let $cnt=10;
while($cnt <= 99)
{
 eval INSERT INTO mutidata2_1 (c1, c2, c3, c4, c5, c6, c7) VALUES  (2023, UNHEX('4A6F686E'), 'Hello', 'World', 'This is a medium blob example.', 'Short text', 'This is a longer text example.'), (2024, UNHEX('4D617279'), 'Hi', 'Blob', 'Another medium blob example.', 'Tiny text', 'This is another longer text example.');
 inc $cnt;
}
let $cnt=0;
while($cnt <= 99)
{
 eval INSERT INTO mutidata3_1 (c1) VALUES  ('例子1'),  ('例子2'),  ('例子3');
 inc $cnt;
}
let $cnt=0;
while($cnt <= 99)
{
 eval INSERT INTO mutidata4_1 (name, hobbies) VALUES  ('张三', '足球,篮球'),  ('李四', '阅读,旅行'), ('王五', '音乐'),  ('赵六', '足球,音乐,旅行'),  ('孙七', '');
 inc $cnt;
}
let $cnt=0;
while($cnt <= 99)
{
  eval INSERT INTO mutidata5_1 (c1, c2, c3, c4, c5, c6, c7, c8) VALUES  ('TINYBLOB data', 'BLOB data with more content.', 'MEDIUMBLOB data with even more content to test larger size.', 'LONGBLOB data for testing very large binary objects.', 'Short text', 'This is a longer text example for TEXT field.', 'This is a MEDIUMTEXT example with much more content to test larger text fields.', 'This is a LONGTEXT example for testing extremely large text fields.');
 eval INSERT INTO mutidata5_1 (c1, c2, c3, c4, c5, c6, c7, c8) VALUES  ('TinyBlob2', 'Blob2 with more data.', 'MediumBlob2 with extended content.', 'LongBlob2 for testing large objects.', 'TinyText2', 'Text2 with extended length.', 'MediumText2 with even more content.', 'LongText2 for testing extreme length.');
 eval INSERT INTO mutidata5_1 (c1, c2, c3, c4, c5, c6, c7, c8) VALUES  ('TinyBlob3', 'Blob3 with additional data.', 'MediumBlob3 with more content.', 'LongBlob3 for testing large objects.', 'TinyText3', 'Text3 with extended length.', 'MediumText3 with even more content.', 'LongText3 for testing extreme length.');
 eval INSERT INTO mutidata5_1 (c1, c2, c3, c4, c5, c6, c7, c8) VALUES  ('TinyBlob4', 'Blob4 with extra data.', 'MediumBlob4 with extended content.', 'LongBlob4 for testing large objects.', 'TinyText4', 'Text4 with extended length.', 'MediumText4 with even more content.', 'LongText4 for testing extreme length.');
 eval INSERT INTO mutidata5_1 (c1, c2, c3, c4, c5, c6, c7, c8) VALUES  ('TinyBlob5', 'Blob5 with more data.', 'MediumBlob5 with extended content.', 'LongBlob5 for testing large objects.', 'TinyText5', 'Text5 with extended length.', 'MediumText5 with even more content.', 'LongText5 for testing extreme length.');
 inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
  eval INSERT INTO mutidata6_1 (id, docs, docs1) VALUES ($cnt,JSON_OBJECT('name', 'Alice', 'age', $cnt, 'city', 'New York'),JSON_OBJECT('hobbies', JSON_ARRAY('reading', 'traveling')) );
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  eval INSERT INTO mutidata7_1 (json_info) VALUES (JSON_OBJECT('id', 101, 'NAME', 'Alice', 'age', 25) );
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  --error 1146,0
  INSERT INTO mutidata8_1 (c1, c2, c3, c4, c5, c6, c7) VALUES ('[1, 2, 3]','["Alice", "Bob", "Charlie"]','[1000000000, 2000000000]','[1, 0, 1]','[1.1, 2.2, 3.3]','[1.11, 2.22, 3.33]','[100, 200, 300]');
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  --error 1146,0,1064
  eval INSERT INTO mutidata9_1 (c1) VALUES ([[[[[[1, 2], [3, 4]], [[5, 6], [7, 8]]], [[[9, 10], [11, 12]], [[13, 14], [15, 16]]]]]]);
  inc $cnt;
}
let $cnt=1000;
while($cnt <= 1500)
{
 eval  INSERT INTO mutidata10_1 (c1, c2) VALUES ($cnt,$cnt);
 inc $cnt;
}
select * from mutidata10_copy_1;
--error 1146,0
select * from mutidata11_1;
--error 1146,0
select * from mutidata12_1;
--error 1146,0
select * from mutidata13_1;
let $cnt=200;
while($cnt <= 500)
{
  eval INSERT INTO mutidata14_1 (col1, col2, col3, col4)  VALUES (1, 3.14,'这是一个测试字符串','固定长度');
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  eval INSERT INTO mutidata15_1 VALUES ();
  eval INSERT INTO mutidata15_1 (c1) VALUES (NULL);
  eval INSERT INTO mutidata15_1 (c1) VALUES ($cnt);
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  eval INSERT INTO mutidata16_1 (col1, col2) VALUES (10, 20);
  --error 3819,0
  eval INSERT INTO mutidata16_1 (col1, col2) VALUES (-5, 20);
  inc $cnt;
}
let $cnt=200;
while($cnt <= 500)
{
  eval INSERT INTO mutidata17_1 (col1, col2) VALUES (10, 1);
  eval INSERT INTO mutidata17_1 (col1, col2) VALUES (20, 2);
  eval INSERT INTO mutidata17_1 (col1, col2) VALUES (30, 3);
  --error 1452,0
  eval INSERT INTO mutidata17_1 (col1, col2) VALUES (40, 4);
  inc $cnt;
}
--enable_all_result_log
