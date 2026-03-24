
# nchar literal n'abc'
# char literal 'abc'

--source mysql_test/include/ignore_drop_table_err.inc
drop table multi_charset_t;

create table multi_charset_t(
c_number number,
c_char char(100),
c_varchar2 varchar2(100),
c_nchar nchar(100),
c_nvarchar2 nvarchar2(100),
c_clob clob,
primary key(c_number)
);

insert into multi_charset_t values(1, 'CHAR_VALUE', 'VARCHAR_VALUE', 'NCHAR_VALUE', 'NVARCHAR_VALUE', 'CLOB_VALUE');
insert into multi_charset_t values(2, '甲', '乙', '丙', '丁', '戊');
insert into multi_charset_t values(3, '甲a', '乙b', '丙c', '丁d', '戊e');

