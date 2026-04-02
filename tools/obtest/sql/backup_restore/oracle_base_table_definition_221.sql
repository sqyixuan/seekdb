CREATE TABLE base_table_1 (
     intType               INT NOT NULL primary key,
     numericType           NUMBER(10,0),
) ;
insert into base_table_1 values(2,9);
insert into base_table_1 values(6,3);
insert into base_table_1 values(7,2);
insert into base_table_1 values(8,11);
insert into base_table_1 values(-2147483648,7);
insert into base_table_1 values(2147483647,7);

create index gindex on base_table_1(numericType) global partition by hash(numericType) partitions 5;
create index lindex on base_table_1(numericType, intType);

