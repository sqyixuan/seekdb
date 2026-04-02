--disable_abort_on_error
eval create table t_ctas as select $column_expr as c1 from $table_name;
select DATA_TYPE, CHAR_LENGTH, CHAR_USED from user_tab_cols where TABLE_NAME = 'T_CTAS' and COLUMN_NAME = 'C1';
drop table t_ctas;
--enable_abort_on_error
