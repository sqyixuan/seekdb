
select asciistr(NULL) from dual;
select asciistr('') from dual;
select asciistr('asdf1𝄞2431𝄞25我们98𝄞!%@𝄞$') from dual;
select asciistr(n'vw𝄞n开始p9ey𝄞tr873𝄞4g3') from dual;
select asciistr(timestamp'1992-12-12 12:12:12.1232') from dual;
select asciistr(23412341234) from dual;

select coercibility(asciistr(23412341234)) from dual;
