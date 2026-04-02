
select to_number('11.234', '900.999') from dual;
select to_number(n'11.234', '900.999') from dual;
select to_number('11.234', n'900.999') from dual;
select to_number(n'11.234', n'900.999') from dual;

select to_number('01.34', '900.009') from dual;
select to_number(n'01.34', '900.009') from dual;
select to_number('01.34', n'900.009') from dual;
select to_number(n'01.34', n'900.009') from dual;

