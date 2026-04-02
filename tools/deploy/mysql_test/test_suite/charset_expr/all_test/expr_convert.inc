
select convert(NULL, 'AL32UTF8') from dual;
select convert('', 'AL32UTF8') from dual;
select convert(date'1934-1-12', 'AL32UTF8') from dual;
select convert(1242325, 'AL32UTF8') from dual;
--error 1482
select convert('', '') from dual;
--error 1482
select convert('', '', '') from dual;

select rawtohex(convert('A', 'AL32UTF8')) from dual;
select rawtohex(convert('A', 'AL16UTF16')) from dual;
select rawtohex(convert('A', 'ZHS16GBK')) from dual;
select rawtohex(convert('A', 'ZHS32GB18030')) from dual;
select rawtohex(convert('A', 'ZHS32GB18030_2022')) from dual;

select rawtohex(convert('A', 'AL32UTF8', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('A', 'AL16UTF16', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('A', 'ZHS16GBK', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('A', 'ZHS32GB18030', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('A', 'ZHS32GB18030_2022', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');

select rawtohex(convert('阿', 'AL32UTF8')) from dual;
select rawtohex(convert('阿', 'AL16UTF16')) from dual;
select rawtohex(convert('阿', 'ZHS16GBK')) from dual;
select rawtohex(convert('阿', 'ZHS32GB18030')) from dual;
select rawtohex(convert('阿', 'ZHS32GB18030_2022')) from dual;

select rawtohex(convert('阿', 'AL32UTF8', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('阿', 'AL16UTF16', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('阿', 'ZHS16GBK', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('阿', 'ZHS32GB18030', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
select rawtohex(convert('阿', 'ZHS32GB18030_2022', src_cs)) from (select value src_cs from v$nls_parameters where parameter = 'NLS_CHARACTERSET');
