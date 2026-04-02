--error 0,942
drop table SMS_SMGTEST;
create table SMS_SMGTEST(msgcontent varchar2(2000));
delimiter /;
CREATE OR REPLACE PROCEDURE func222
 IS
v_msg            varchar2(2000);
v_sql            varchar2(2000);
BEGIN
  v_msg := '我们';
  v_sql := 'insert into SMS_SMGTEST(msgcontent) values (''' ||v_msg || ''')';
  execute immediate v_sql;
END;
/
delimiter ;/
call func222();
select * from SMS_SMGTEST;
drop table SMS_SMGTEST;
