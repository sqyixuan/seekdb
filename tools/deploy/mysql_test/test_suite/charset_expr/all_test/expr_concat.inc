
--source mysql_test/test_suite/charset_expr/include/create_table.inc

select concat('a', 'b') R from dual;
select concat(n'a', 'b') R  from dual;
select concat('a', n'b') R from dual;
select concat(n'a', n'b') R from dual;
select concat(1, 'b') R from dual;
select concat('a', 123) R from dual;
select concat(1, n'b') R from dual;
select concat(n'a', 123) R from dual;
select concat(1123, 124123) R from dual;
select concat(date '2019-12-31', ' is data value') R from dual;
select concat(date '2019-12-31', n' 是日期') R from dual;
select concat(timestamp '2019-12-31 23:59:59', ' is timestamp value') R from dual;
select concat(timestamp '2019-12-31 23:59:59', n' 是时间戳') R from dual;
select concat(interval '1-1' year to month, ' is interval ym value') R from dual;
select concat(interval '1-1' year to month, n' 是年月时间段') R from dual;
select concat(interval '1 1:1:1' day to second, ' is interval ds value') R from dual;
select concat(interval '1 1:1:1' day to second, n' 是日时时间段') R from dual;

select concat(c_char, c_char) R from multi_charset_t;
select concat(c_char, c_varchar2) R from multi_charset_t;
select concat(c_char, c_nchar) R from multi_charset_t;
select concat(c_char, c_nvarchar2) R from multi_charset_t;
#select concat(c_char, c_clob) R from multi_charset_t;

select concat(c_varchar2, c_char) R from multi_charset_t;
select concat(c_varchar2, c_varchar2) R from multi_charset_t;
select concat(c_varchar2, c_nchar) R from multi_charset_t;
select concat(c_varchar2, c_nvarchar2) R from multi_charset_t;
#select concat(c_varchar2, c_clob) R from multi_charset_t;

select concat(c_nchar, c_char) R from multi_charset_t;
select concat(c_nchar, c_varchar2) R from multi_charset_t;
select concat(c_nchar, c_nchar) R from multi_charset_t;
select concat(c_nchar, c_nvarchar2) R from multi_charset_t;
#--error 600
#select concat(c_nchar, c_clob) R from multi_charset_t;

select concat(c_nvarchar2, c_char) R from multi_charset_t;
select concat(c_nvarchar2, c_varchar2) R from multi_charset_t;
select concat(c_nvarchar2, c_nchar) R from multi_charset_t;
select concat(c_nvarchar2, c_nvarchar2) R from multi_charset_t;
#--error 600
#select concat(c_nvarchar2, c_clob) R from multi_charset_t;

#select concat(c_clob, c_char) R from multi_charset_t;
#select concat(c_clob, c_varchar2) R from multi_charset_t;
#--error 600
#select concat(c_clob, c_nchar) R from multi_charset_t;
#--error 600
#select concat(c_clob, c_nvarchar2) R from multi_charset_t;
#select concat(c_clob, c_clob) R from multi_charset_t;

