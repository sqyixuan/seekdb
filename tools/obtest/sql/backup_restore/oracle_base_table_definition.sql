CREATE TABLE base_table_1 (
     intType               INT NOT NULL primary key,
     numeric1Type           NUMBER(10,2) default 12.34,
     numeric2Type           NUMBER default 12.34,
     numeric3Type           NUMBER(10) default 12.34,
     floatType              float(10) default 12.34,
     binaryfloatType        BINARY_FLOAT default 12.34,
     binarydoubleType       BINARY_DOUBLE default 12.34,
     dateType               date default Date '2012-12-04',
     timestampType          timestamp(8) default Timestamp '2012-12-04 12:02:03.123456789',
     timestamptzType        timestamp(8) with time zone default Timestamp '2012-12-04 12:02:03.123456789',
     timestampltzType       timestamp(8) with local time zone default Timestamp '2012-12-04 12:02:03.123456789',
     varchar2Type           varchar(100 byte) default 'hello',
     charType               char(100 char) default 'hello',
     rawType                raw(100) default '61626364',
     nvarchar2Type          nvarchar2(100) default 'hello',
     ncharType              nchar(100) default 'hello',
     intervalydType         interval year to month default interval '13-0' year to month,
     intervaldsType         interval day to second default interval '99 23:59:59.654321' day to second
) ;

insert into base_table_1 (intType, numeric1Type) values(2,9.313333337);
insert into base_table_1 (intType, numeric1Type) values(7,2.113333337);
insert into base_table_1 (intType, numeric1Type) values(8,11.193333337);
insert into base_table_1 (intType, numeric1Type) values(-1,7.413333337);

#bug for mytest
set @@sql_mode='PIPES_AS_CONCAT,STRICT_ALL_TABLES,PAD_CHAR_TO_FULL_LENGTH';

update base_table_1 set numeric2Type= numeric1Type * 100;
update base_table_1 set numeric3Type= numeric1Type * 100;
update base_table_1 set floatType= numeric1Type;
update base_table_1 set binaryfloatType= numeric1Type;
update base_table_1 set binarydoubleType= numeric1Type;
update base_table_1 set dateType= Date '2012-12-01' + numeric1Type;
update base_table_1 set timestampType= to_timestamp(dateType) + interval'19 23:59:59.123456789' day(9) to second(9);
update base_table_1 set timestamptzType= to_timestamp(dateType) + interval'19 23:59:59.123456789' day(9) to second(9);
update base_table_1 set timestampltzType= to_timestamp(dateType) + interval'19 23:59:59.123456789' day(9) to second(9);
update base_table_1 set varchar2Type= to_char(numeric1Type);
update base_table_1 set charType= varchar2Type;
update base_table_1 set rawType= utl_raw_cast_to_raw(to_char(numeric1Type));
update base_table_1 set nvarchar2Type= numeric1Type;
update base_table_1 set ncharType= numeric1Type;
update base_table_1 set intervalydType= '12-11';
update base_table_1 set intervaldsType= timestampType - dateType;

insert into base_table_1 (intType) values(100);

create index gindex on base_table_1(numeric1Type) global partition by hash(numeric1Type) partitions 5;
create index lindex on base_table_1(numeric1Type, intType);

create table base_table_default_value_with_escape_character(c1 char(10) default '\\', c2 varchar(10) default '\\');
