

--source mysql_test/include/ignore_drop_table_err.inc
drop table t_charset_nvarchar2;
--source mysql_test/include/ignore_drop_table_err.inc
drop table t_charset_nchar;
--source mysql_test/include/ignore_drop_table_err.inc
drop table t_charset_varchar2;
--source mysql_test/include/ignore_drop_table_err.inc
drop table t_charset_char;

create table t_charset_nvarchar2(c1 nvarchar2(10) primary key);
insert into t_charset_nvarchar2 values('A');
insert into t_charset_nvarchar2 values('A ');
insert into t_charset_nvarchar2 values('A'||chr(1));
insert into t_charset_nvarchar2 values('AZ');
select rawtohex(c1) from t_charset_nvarchar2;
drop table t_charset_nvarchar2;

create table t_charset_nchar(c1 nchar(10) primary key);
insert into t_charset_nchar values('A');
--source mysql_test/include/replace_object_name_created_by_sys.inc
--error 00001
insert into t_charset_nchar values('A ');
insert into t_charset_nchar values('A'||chr(1));
insert into t_charset_nchar values('AZ');
select rawtohex(c1) from t_charset_nchar;
drop table t_charset_nchar;

create table t_charset_varchar2(c1 varchar2(10) primary key);
insert into t_charset_varchar2 values('A');
insert into t_charset_varchar2 values('A ');
insert into t_charset_varchar2 values('A'||chr(1));
insert into t_charset_varchar2 values('AZ');
select rawtohex(c1) from t_charset_varchar2;
drop table t_charset_varchar2;

create table t_charset_char(c1 char(10) primary key);
insert into t_charset_char values('A');
--source mysql_test/include/replace_object_name_created_by_sys.inc
--error 00001
insert into t_charset_char values('A ');
insert into t_charset_char values('A'||chr(1));
insert into t_charset_char values('AZ');
select rawtohex(c1) from t_charset_char;
drop table t_charset_char;


