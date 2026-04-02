SELECT length(RPAD('Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ ⅰ ⅱ ⅲ ⅳ ⅴ ⅵ ⅶ ⅷ ⅸ ⅹ', 10, '$')) R FROM dual;
select lpad(n'123', 10, '中') R from dual;
select rpad(n'123', 10, '中') R from dual;
select rpad('ΤΥΦΧΨΩ',10,'1103') R from dual;
