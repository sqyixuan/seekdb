CREATE TABLE MyTable (t1 int,t2 int);
--disable_all_result_log
let $cnt=1;
while($cnt <= 60)
{
  eval ALTER TABLE MyTable ADD column_$cnt varchar(255);
  inc $cnt;
}
--enable_all_result_log
--error 1071,0
ALTER TABLE MyTable ADD CONSTRAINT pk_MyTable PRIMARY KEY (column_1,column_2,column_3,column_4,column_5,column_6,column_7,column_8,column_9,column_10,column_11,column_12,column_13,column_14,column_15,column_16,column_17,column_18,column_19,column_20,column_21,column_22,column_23,column_24,column_25,column_26,column_27,column_28,column_29,column_30,column_31,column_32,column_33,column_34,column_35,column_36,column_37,column_38,column_39,column_40,column_41,column_42,column_43,column_44,column_45,column_46,column_47,column_48,column_49,column_50,column_51,column_52,column_53,column_54,column_55,column_56,column_57,column_58,column_59,column_60);
--error 1068,0
ALTER TABLE MyTable ADD CONSTRAINT pk_MyTable PRIMARY KEY (column_1,column_2,column_3,column_4,column_5,column_6,column_7,column_8,column_9,column_10);
INSERT INTO MyTable (t1,t2,column_1,column_2,column_3,column_4,column_5,column_6,column_7,column_8,column_9,column_10,column_11,column_12,column_13,column_14,column_15,column_16,column_17,column_18,column_19,column_20,column_21,column_22,column_23,column_24,column_25,column_26,column_27,column_28,column_29,column_30,column_31,column_32,column_33,column_34,column_35,column_36,column_37,column_38,column_39,column_40,column_41,column_42,column_43,column_44,column_45,column_46,column_47,column_48,column_49,column_50,column_51,column_52,column_53,column_54,column_55,column_56,column_57,column_58,column_59,column_60) values (1,2,'data1','data2','data3','data4','data5','data6','data7','data8','data9','data10','data11','data12','data13','data14','data15','data16','data17','data18','data19','data20','data21','data22','data23','data24','data25','data26','data27','data28','data29','data30','data31','data32','data33','data34','data35','data36','data37','data38','data39','data40','data41','data42','data43','data44','data45','data46','data47','data48','data49','data50','data51','data52','data53','data54','data55','data56','data57','data58','data59','data60');
