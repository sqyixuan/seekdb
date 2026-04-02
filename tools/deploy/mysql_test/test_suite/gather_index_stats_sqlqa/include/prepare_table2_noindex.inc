
ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
set ob_trx_timeout = 10000000000;
set ob_query_timeout = 10000000000;

--echo ###无主键、唯一键、索引表,普通表、均匀分布###

--error 0,942
DROP TABLE general_tab_uniform purge;
CREATE TABLE general_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20));
delimiter /;
BEGIN
	FOR i IN 1..10 LOOP
		INSERT INTO general_tab_uniform VALUES(5, 5.12345,5.12345,5.12345, 'admin5', 'test5', 'admin5', 'test5');
		INSERT INTO general_tab_uniform VALUES(10, 10.12345,10.12345,10.12345, 'admin10', NULL, 'admin10', NULL);
		INSERT INTO general_tab_uniform VALUES(15, 15.12345,15.12345,15.12345, 'admin15', 'test15', 'admin15', 'test15');
		INSERT INTO general_tab_uniform VALUES(20, 20.12345,20.12345,20.12345, 'admin20', 'test20', 'admin20', 'test20');
		INSERT INTO general_tab_uniform VALUES(25, 25.12345,25.12345,25.12345, 'admin25', 'test25', 'admin25', 'test25');
		INSERT INTO general_tab_uniform VALUES(30, 30.12345,30.12345,30.12345, 'admin30', 'test30', 'admin30', 'test30');
		INSERT INTO general_tab_uniform VALUES(35, 35.12345,35.12345,35.12345, 'admin35', 'test35', 'admin35', 'test35');
		INSERT INTO general_tab_uniform VALUES(40, NULL,    NULL,    NULL,     'admin40', 'test40', 'admin40', 'test40');
		INSERT INTO general_tab_uniform VALUES(45, 45.12345,45.12345,45.12345, 'admin45', 'test45', 'admin45', 'test45');
		INSERT INTO general_tab_uniform VALUES(50, 50.12345,50.12345,50.12345, 'admin50', 'test50', 'admin50', 'test50');
		INSERT INTO general_tab_uniform VALUES(55, 55.12345,55.12345,55.12345, 'admin55', 'test55', 'admin55', 'test55');
		INSERT INTO general_tab_uniform VALUES(60, 60.12345,60.12345,60.12345,  NULL, 'test60'    ,  NULL, 'test60'    );
		INSERT INTO general_tab_uniform VALUES(65, 65.12345,65.12345,65.12345, 'admin65', 'test65', 'admin65', 'test65');
		INSERT INTO general_tab_uniform VALUES(70, 70.12345,70.12345,70.12345, 'admin70', 'test70', 'admin70', 'test70');
		INSERT INTO general_tab_uniform VALUES(75, 75.12345,75.12345,75.12345, 'admin75', 'test75', 'admin75', 'test75');
		INSERT INTO general_tab_uniform VALUES(80, 80.12345,80.12345,80.12345, 'admin80', 'test80', 'admin80', 'test80');
		INSERT INTO general_tab_uniform VALUES(85, 85.12345,85.12345,85.12345, 'admin85', 'test85', 'admin85', 'test85');
		INSERT INTO general_tab_uniform VALUES(90, 90.12345,90.12345,90.12345, 'admin90', 'test90', 'admin90', 'test90');
		INSERT INTO general_tab_uniform VALUES(95, 95.12345,95.12345,95.12345, 'admin95', 'test95', 'admin95', 'test95');
		INSERT INTO general_tab_uniform VALUES(NULL, 100.12345,100.12345,100.12345, 'admin100', 'test100', 'admin100', 'test100');
	END LOOP;
END;
/
delimiter ;/

--echo ###无主键、唯一键、索引表,普通表、高斯分布###
--error 0,942
DROP TABLE general_tab_Gaussian purge;
CREATE TABLE general_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20));
delimiter /;
BEGIN
	INSERT INTO general_tab_Gaussian     VALUES(2,  2.12345,  2.12345 ,  2.12345, 'admin2', 'test2'  , 'admin2', 'test2'  );
	FOR i IN 1..4 LOOP                                                                     
		INSERT INTO general_tab_Gaussian VALUES(3,  3.12345,  3.12345 ,  3.12345 , 'admin3', 'test3'  , 'admin3', 'test3'  );
	END LOOP;                                                                              
	FOR i IN 1..25 LOOP                                                                    
		INSERT INTO general_tab_Gaussian VALUES(10, 10.12345, 10.12345, 10.12345, 'admin10', 'test10', 'admin10', 'test10');
		INSERT INTO general_tab_Gaussian VALUES(20, 20.12345, 20.12345, 20.12345, 'admin20', 'test20', 'admin20', 'test20');
	END LOOP;                                                                                         
	FOR i IN 1..3 LOOP                                                                                
		INSERT INTO general_tab_Gaussian VALUES(60, 60.12345, 60.12345, 60.12345, 'admin60', 'test60', 'admin60', 'test60');
	END LOOP;                                                                                         
	FOR i IN 1..17 LOOP                                                                               
		INSERT INTO general_tab_Gaussian VALUES(70, 70.12345, 70.12345, 70.12345, 'admin70', 'test70', 'admin70', 'test70');
	END LOOP;                                                                              
	FOR i IN 1..5 LOOP                                                                     
		INSERT INTO general_tab_Gaussian VALUES(77, NULL,     NULL    ,NULL    ,     NULL, 'test77' ,     NULL, 'test77' );
	END LOOP;                                                                              
	FOR i IN 1..18 LOOP                                                                    
		INSERT INTO general_tab_Gaussian VALUES(82, 82.12345, 82.12345,82.12345, 'admin82', 'test82', 'admin82', 'test82');
	END LOOP;                                                                              
	FOR i IN 1..30 LOOP                                                                     
		INSERT INTO general_tab_Gaussian VALUES(86, 86.12345, 86.12345,86.12345, 'admin86', NULL    , 'admin86', NULL     );
	END LOOP;
	FOR i IN 1..2 LOOP
		INSERT INTO general_tab_Gaussian VALUES(NULL, 95.12345,95.12345,95.12345, 'admin95', 'test95', 'admin95', 'test95');
	END LOOP;	
END;
/
delimiter ;/

--echo ###无主键、唯一键、索引表,range分区表、均匀分布###
--error 0,942
DROP TABLE partitioned_range_tab_uniform purge;
CREATE TABLE partitioned_range_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
PARTITION BY RANGE(f1) (PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO partitioned_range_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,range分区表、高斯分布###
--error 0,942
DROP TABLE partitioned_range_tab_Gaussian purge;
CREATE TABLE partitioned_range_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
PARTITION BY RANGE(f1) (PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO partitioned_range_tab_Gaussian SELECT * FROM general_tab_Gaussian;

--echo ###无主键、唯一键、索引表,hash分区表、均匀分布###
--error 0,942
DROP TABLE partitioned_hash_tab_uniform purge;
CREATE TABLE partitioned_hash_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by hash(f5) partitions 4;
INSERT INTO partitioned_hash_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,hash分区表、高斯分布###
--error 0,942
DROP TABLE partitioned_hash_tab_Gaussian purge;
CREATE TABLE partitioned_hash_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by hash(f4) partitions 4;
INSERT INTO partitioned_hash_tab_Gaussian SELECT * FROM general_tab_Gaussian;

--echo ###无主键、唯一键、索引表,list分区表、均匀分布###
--error 0,942
DROP TABLE partitioned_list_tab_uniform purge;
CREATE TABLE partitioned_list_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f2)
(partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default)
);
INSERT INTO partitioned_list_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,list分区表、高斯分布###

--error 0,942
DROP TABLE partitioned_list_tab_Gaussian purge;
CREATE TABLE partitioned_list_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f2)
(partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default)
);
INSERT INTO partitioned_list_tab_Gaussian SELECT * FROM general_tab_Gaussian;

--echo ###无主键、唯一键、索引表,range-hash、均匀分布###
--error 0,942
DROP TABLE partitioned_rangehash_tab_uniform purge;
CREATE TABLE partitioned_rangehash_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by range(f1)
subpartition by hash(f4) subpartitions 4
(PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO partitioned_rangehash_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,range-hash、高斯分布###

--error 0,942
DROP TABLE partitioned_rangehash_tab_Gaussian purge;
CREATE TABLE partitioned_rangehash_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by range(f1)
subpartition by hash(f4) subpartitions 4
(PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO partitioned_rangehash_tab_Gaussian SELECT * FROM general_tab_Gaussian;

--echo ###无主键、唯一键、索引表,range-list、均匀分布###
--error 0,942
DROP TABLE partitioned_rangelist_tab_uniform purge;
CREATE TABLE partitioned_rangelist_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by range(f1)
subpartition by list(f4)
subpartition template
(
subpartition part1 values ('2.12345'),
subpartition part2 values ('10.12345'),
subpartition part3 values ('20.12345'),
subpartition part4 values ('50.12345'),
subpartition part5 values ('95.12345'),
subpartition part6 values (default))
(PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO partitioned_rangelist_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,range-list、高斯分布###

--error 0,942
DROP TABLE partitioned_rangelist_tab_Gaussian purge;
CREATE TABLE partitioned_rangelist_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by range(f1)
subpartition by list(f4)
subpartition template
(
subpartition part1 values ('2.12345'),
subpartition part2 values ('10.12345'),
subpartition part3 values ('20.12345'),
subpartition part4 values ('50.12345'),
subpartition part5 values ('95.12345'),
subpartition part6 values (default))
(PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO partitioned_rangelist_tab_Gaussian SELECT * FROM general_tab_Gaussian;


--echo ###无主键、唯一键、索引表,list-range、均匀分布###

--error 0,942
DROP TABLE partitioned_listrange_tab_uniform purge;
CREATE TABLE partitioned_listrange_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by range(f1)
subpartition template
(subpartition p1 VALUES less than(6),
subpartition p2 VALUES less than(81),
subpartition p3 VALUES less than(MAXVALUE))
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;

INSERT INTO partitioned_listrange_tab_uniform SELECT * FROM general_tab_uniform;


--echo ###无主键、唯一键、索引表,list-range、高斯分布###

--error 0,942
DROP TABLE partitioned_listrange_tab_Gaussian purge;
CREATE TABLE partitioned_listrange_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by range(f1)
subpartition template
(subpartition p1 VALUES less than(6),
subpartition p2 VALUES less than(81),
subpartition p3 VALUES less than(MAXVALUE))
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;

INSERT INTO partitioned_listrange_tab_Gaussian SELECT * FROM general_tab_Gaussian;


--echo ###无主键、唯一键、索引表,list-range、空表###
--error 0,942
DROP TABLE partitioned_listrange_tab_Gaussian2 purge;
CREATE TABLE partitioned_listrange_tab_Gaussian2 (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by range(f1)
subpartition template
(subpartition p1 VALUES less than(6),
subpartition p2 VALUES less than(81),
subpartition p3 VALUES less than(MAXVALUE))
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;


--echo ###无主键、唯一键、索引表,list-range、多字段相同表###
--error 0,942
DROP TABLE partitioned_listrange_tab_uniform2 purge;
CREATE TABLE partitioned_listrange_tab_uniform2 (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by range(f1)
subpartition template
(subpartition p1 VALUES less than(6),
subpartition p2 VALUES less than(81),
subpartition p3 VALUES less than(MAXVALUE))
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;

delimiter /;
BEGIN
	FOR i IN 1..10 LOOP
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (5, 5.12345,5.12345,5.12345, 'admin5', 'test5', 'admin5', 'test5');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (10, 10.12345,10.12345,10.12345, 'admin10', NULL, 'admin10', NULL);
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (15, 15.12345,15.12345,15.12345, 'admin15', 'test15', 'admin15', 'test15');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (20, 20.12345,20.12345,20.12345, 'admin20', 'test20', 'admin20', 'test20');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (25, 25.12345,25.12345,25.12345, 'admin25', 'test25', 'admin25', 'test25');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (30, 30.12345,30.12345,30.12345, 'admin30', 'test30', 'admin30', 'test30');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (35, 35.12345,35.12345,35.12345, 'admin35', 'test35', 'admin35', 'test35');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (40, NULL,    NULL,    NULL,     'admin40', 'test40', 'admin40', 'test40');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (45, 45.12345,45.12345,45.12345, 'admin45', 'test45', 'admin45', 'test45');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (50, 50.12345,50.12345,50.12345, 'admin50', 'test50', 'admin50', 'test50');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (55, 55.12345,55.12345,55.12345, 'admin55', 'test55', 'admin55', 'test55');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (60, 60.12345,60.12345,60.12345,  NULL, 'test60'    ,  NULL, 'test60'    );
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (65, 65.12345,65.12345,65.12345, 'admin65', 'test65', 'admin65', 'test65');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (70, 70.12345,70.12345,70.12345, 'admin70', 'test70', 'admin70', 'test70');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (75, 75.12345,75.12345,75.12345, 'admin75', 'test75', 'admin75', 'test75');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (80, 80.12345,80.12345,80.12345, 'admin80', 'test80', 'admin80', 'test80');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (85, 85.12345,85.12345,85.12345, 'admin85', 'test85', 'admin85', 'test85');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (90, 90.12345,90.12345,90.12345, 'admin90', 'test90', 'admin90', 'test90');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (95, 95.12345,95.12345,95.12345, 'admin95', 'test95', 'admin95', 'test95');
		INSERT INTO partitioned_listrange_tab_uniform2 VALUES (NULL, 100.12345,100.12345,100.12345, 'admin100', 'test100', 'admin100', 'test100');
	END LOOP;
END;
/
delimiter ;/

--echo ###无主键、唯一键、索引表,list-hash、均匀分布###
--error 0,942
DROP TABLE partitioned_listhash_tab_uniform purge;
CREATE TABLE partitioned_listhash_tab_uniform (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by hash(f1)
subpartitions 4
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;

INSERT INTO partitioned_listhash_tab_uniform SELECT * FROM general_tab_uniform;

--echo ###无主键、唯一键、索引表,list-hash、高斯分布###

--error 0,942
DROP TABLE partitioned_listhash_tab_Gaussian purge;
CREATE TABLE partitioned_listhash_tab_Gaussian (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20))
partition by list(f4)
subpartition by hash(f1)
subpartitions 4
(
partition part1 values ('2.12345'),
partition part2 values ('10.12345'),
partition part3 values ('20.12345'),
partition part4 values ('50.12345'),
partition part5 values ('95.12345'),
partition part6 values (default))
;

INSERT INTO partitioned_listhash_tab_Gaussian SELECT * FROM general_tab_Gaussian;


--echo ###无主键、唯一键、索引表,虚拟列，COMPOSITE_PARTITIONED_TAB,两级分区###

--error 0,2289
DROP SEQUENCE SEQ1;
CREATE SEQUENCE SEQ1 MINVALUE 1 MAXVALUE 999999999999999 START WITH 1 INCREMENT BY 1 cache 201;
--error 0,942
DROP TABLE COMPOSITE_PARTITIONED_TAB purge;
CREATE TABLE COMPOSITE_PARTITIONED_TAB(f1 int, f2 binary_float invisible AS (f1+0) virtual, f3 char(10), f4 varchar2(10) invisible AS (f3||NULL) virtual, f5 NUMBER PRIMARY KEY, f6 nvarchar2(10) UNIQUE)
PARTITION BY RANGE(f5) subpartition BY list(f5)
(PARTITION p1 VALUES less than(6) (subpartition sub1 VALUES(1,2),subpartition sub2 VALUES(DEFAULT)),
PARTITION p2 VALUES less than(81) (subpartition sub3 VALUES(20,25,26),subpartition sub4 VALUES(DEFAULT)),
PARTITION p3 VALUES less than(MAXVALUE) (subpartition sub5 VALUES(90,100),subpartition sub6 VALUES(DEFAULT)));

--error 0,942
DROP TABLE tmp_COMPOSITE_PARTITIONED_TAB purge;
create table tmp_COMPOSITE_PARTITIONED_TAB as select f1 as f1,f4,f5 as f3,f6 FROM general_tab_Gaussian;

INSERT ALL INTO COMPOSITE_PARTITIONED_TAB VALUES (f1, f3, SEQ1.NEXTVAL, 'abc'||SEQ1.NEXTVAL) SELECT * from tmp_COMPOSITE_PARTITIONED_TAB ORDER BY 1,2,3,4;

--echo ###无主键、唯一键、索引表,虚拟列，COMPOSITE_PARTITIONED_TAB，一级分区###
--error 0,2289
DROP SEQUENCE SEQ2;
CREATE SEQUENCE SEQ2 MINVALUE 1 MAXVALUE 999999999999999 START WITH 1 INCREMENT BY 1 cache 201;
--error 0,942
DROP TABLE PARTITIONED_TAB purge;
CREATE TABLE PARTITIONED_TAB(f1 int, f2 binary_float invisible AS (f1+0) virtual, f3 char(10), f4 varchar2(10) invisible AS (f3||NULL) virtual, f5 NUMBER PRIMARY KEY, f6 nvarchar2(10) UNIQUE)
PARTITION BY RANGE(f5)(PARTITION p1 VALUES less than(6),PARTITION p2 VALUES less than(81),PARTITION p3 VALUES less than(MAXVALUE));
INSERT ALL INTO PARTITIONED_TAB VALUES(f1, f3, SEQ2.NEXTVAL, 'abc'||SEQ2.NEXTVAL) SELECT * from tmp_COMPOSITE_PARTITIONED_TAB ORDER BY 1,2,3,4;

--echo ###无主键、唯一键、索引表,虚拟列，COMPOSITE_PARTITIONED_TAB，普通表###
--error 0,2289
DROP SEQUENCE SEQ3;
CREATE SEQUENCE SEQ3 MINVALUE 1 MAXVALUE 999999999999999 START WITH 1 INCREMENT BY 1 cache 201;
--error 0,942
DROP TABLE GENERAL_TAB purge;
CREATE TABLE GENERAL_TAB(f1 int, f2 binary_float invisible AS (f1+0) virtual, f3 char(10), f4 varchar2(10) invisible AS (f3||NULL) virtual, f5 NUMBER PRIMARY KEY, f6 nvarchar2(10) UNIQUE);
INSERT ALL INTO GENERAL_TAB VALUES(f1, f3, SEQ1.NEXTVAL, 'abc'||SEQ1.NEXTVAL) SELECT * from tmp_COMPOSITE_PARTITIONED_TAB ORDER BY 1,2,3,4;


--echo ###覆盖三种直方图，频率直方图，捅数量设置20+ 即可，桶的数量设置为10，为混合直方图###

--error 0,942
DROP TABLE general_tab_fre purge;
CREATE TABLE general_tab_fre (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20));
delimiter /;
BEGIN
	FOR i IN 1..10 LOOP
		INSERT INTO general_tab_fre VALUES(5, 5.12345,5.12345,5.12345, 'admin5', 'test5', 'admin5', 'test5');
		INSERT INTO general_tab_fre VALUES(10, 10.12345,10.12345,10.12345, 'admin10', NULL, 'admin10', NULL);
		INSERT INTO general_tab_fre VALUES(15, 15.12345,15.12345,15.12345, 'admin15', 'test15', 'admin15', 'test15');
		INSERT INTO general_tab_fre VALUES(20, 20.12345,20.12345,20.12345, 'admin20', 'test20', 'admin20', 'test20');
		INSERT INTO general_tab_fre VALUES(25, 25.12345,25.12345,25.12345, 'admin25', 'test25', 'admin25', 'test25');
		INSERT INTO general_tab_fre VALUES(30, 30.12345,30.12345,30.12345, 'admin30', 'test30', 'admin30', 'test30');
		INSERT INTO general_tab_fre VALUES(35, 35.12345,35.12345,35.12345, 'admin35', 'test35', 'admin35', 'test35');
		INSERT INTO general_tab_fre VALUES(40, NULL,    NULL,    NULL,     'admin40', 'test40', 'admin40', 'test40');
		INSERT INTO general_tab_fre VALUES(45, 45.12345,45.12345,45.12345, 'admin45', 'test45', 'admin45', 'test45');
		INSERT INTO general_tab_fre VALUES(50, 50.12345,50.12345,50.12345, 'admin50', 'test50', 'admin50', 'test50');
		INSERT INTO general_tab_fre VALUES(55, 55.12345,55.12345,55.12345, 'admin55', 'test55', 'admin55', 'test55');
		INSERT INTO general_tab_fre VALUES(60, 60.12345,60.12345,60.12345,  NULL, 'test60'    ,  NULL, 'test60'    );
		INSERT INTO general_tab_fre VALUES(65, 65.12345,65.12345,65.12345, 'admin65', 'test65', 'admin65', 'test65');
		INSERT INTO general_tab_fre VALUES(70, 70.12345,70.12345,70.12345, 'admin70', 'test70', 'admin70', 'test70');
		INSERT INTO general_tab_fre VALUES(75, 75.12345,75.12345,75.12345, 'admin75', 'test75', 'admin75', 'test75');
		INSERT INTO general_tab_fre VALUES(80, 80.12345,80.12345,80.12345, 'admin80', 'test80', 'admin80', 'test80');
		INSERT INTO general_tab_fre VALUES(85, 85.12345,85.12345,85.12345, 'admin85', 'test85', 'admin85', 'test85');
		INSERT INTO general_tab_fre VALUES(90, 90.12345,90.12345,90.12345, 'admin90', 'test90', 'admin90', 'test90');
		INSERT INTO general_tab_fre VALUES(95, 95.12345,95.12345,95.12345, 'admin95', 'test95', 'admin95', 'test95');
		INSERT INTO general_tab_fre VALUES(NULL, 100.12345,100.12345,100.12345, 'admin100', 'test100', 'admin100', 'test100');
	END LOOP;
END;
/
delimiter ;/

--echo ###top k直方图，桶的个数小于10，比如为8即可为topk直方图###
--error 0,942
DROP TABLE general_tab_topk purge;
CREATE TABLE general_tab_topk (f1 int,f2 float, f3 binary_float,f4 BINARY_DOUBLE, f5 char(10), f6 varchar2(10), f7 nchar(20), f8 nvarchar2(20));
delimiter /;
BEGIN
	INSERT INTO general_tab_topk     VALUES(2,  2.12345,  2.12345 ,  2.12345, 'admin2', 'test2'  , 'admin2', 'test2'  );
	FOR i IN 1..2 LOOP                                                                     
		INSERT INTO general_tab_topk VALUES(3,  3.12345,  3.12345 ,  3.12345 , 'admin3', 'test3'  , 'admin3', 'test3'  );
	END LOOP;                                                                              
	FOR i IN 1..250 LOOP                                                                    
		INSERT INTO general_tab_topk VALUES(10, 10.12345, 10.12345, 10.12345, 'admin10', 'test10', 'admin10', 'test10');
		INSERT INTO general_tab_topk VALUES(20, 20.12345, 20.12345, 20.12345, 'admin20', 'test20', 'admin20', 'test20');
	END LOOP;                                                                                         
	FOR i IN 1..30 LOOP                                                                                
		INSERT INTO general_tab_topk VALUES(60, 60.12345, 60.12345, 60.12345, 'admin60', 'test60', 'admin60', 'test60');
	END LOOP;                                                                                         
	FOR i IN 1..170 LOOP                                                                               
		INSERT INTO general_tab_topk VALUES(70, 70.12345, 70.12345, 70.12345, 'admin70', 'test70', 'admin70', 'test70');
	END LOOP;                                                                              
	FOR i IN 1..20 LOOP                                                                     
		INSERT INTO general_tab_topk VALUES(77, NULL,     NULL    ,NULL    ,     NULL, 'test77' ,     NULL, 'test77' );
	END LOOP;                                                                              
	FOR i IN 1..20 LOOP                                                                    
		INSERT INTO general_tab_topk VALUES(82, 82.12345, 82.12345,82.12345, 'admin82', 'test82', 'admin82', 'test82');
	END LOOP;                                                                              
	FOR i IN 1..30 LOOP                                                                     
		INSERT INTO general_tab_topk VALUES(86, 86.12345, 86.12345,86.12345, 'admin86', NULL    , 'admin86', NULL     );
	END LOOP;
	FOR i IN 1..2 LOOP
		INSERT INTO general_tab_topk VALUES(NULL, 95.12345,95.12345,95.12345, 'admin95', 'test95', 'admin95', 'test95');
	END LOOP;	
END;
/
delimiter ;/


commit;
