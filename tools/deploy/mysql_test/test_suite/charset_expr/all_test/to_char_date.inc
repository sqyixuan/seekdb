set timestamp = 1600000000;
--error 0, 942
drop table t;
create table t(str1 varchar(35), str2 nchar(35), fmt1 varchar(35), fmt2 nchar(35));
insert into t values('2020年1月1日 12:00:00', '2020年1月1日 12:00:00', 'YYYY"年"MM"月"DD"日" HH24:MI:SS', 'YYYY"年"MM"月"DD"日" HH24:MI:SS');
select to_char(sysdate, 'YYYY"年"MM"月"DD"日" HH24:MI:SS') from dual;
select to_char(sysdate, fmt1) from t;
select to_char(sysdate, fmt2) from t;

select to_nchar(sysdate, 'YYYY"年"MM"月"DD"日" HH24:MI:SS') from dual;
select to_nchar(sysdate, fmt1) from t;
select to_nchar(sysdate, fmt2) from t;

select to_date(str1, fmt1) from t;
select to_date(str1, fmt2) from t;
select to_date(str2, fmt1) from t;
select to_date(str2, fmt2) from t;
select to_date('2020年1月1日 12:00:00', 'YYYY"年"MM"月"DD"日" HH24:MI:SS') from dual;
drop table t;