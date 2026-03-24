
select unistr(NULL) from dual;
select unistr('') from dual;
select unistr('ab\\cd\6D4BEFGH\\\8BD5hello\\程序') from dual;
select unistr(timestamp'1992-12-12 12:12:12.1232') from dual;
select unistr(23412341234) from dual;
select unistr('\D834\DD1E') from dual;
select unistr(n'\D834\DD1E') from dual;
SELECT UNISTR(U'\0041') FROM dual;   
SELECT UNISTR(U'\\0041') FROM dual;
SELECT UNISTR(U'\005c0041') FROM dual;

--error 30186
select unistr('\a') from dual;
--error 30186
select unistr('\abc') from dual;
--error 30186
select unistr('\abc*') from dual;
--ERROR 30186
SELECT UNISTR(N'%_\%') FROM dual;  


select coercibility(unistr(23412341234)) from dual;
