--echo ##################################函数场景测试##################################
delimiter //;
--echo ######函数-一般赋值运算-字符表达式,NVARCHAR2######
create or replace function fun1
return NVARCHAR2
 is
v_char1 NVARCHAR2(20);
v_char3 NVARCHAR2(20);
v_char4 NVARCHAR2(20);
v_char5 NVARCHAR2(20);
v_char6 NVARCHAR2(20);
begin
v_char1:=to_char(1234.34,'9,,,,,,,,,,99,9.99');
v_char3:=to_char(1234.34,'9,,,,,,,,,,99,9.99');
v_char4:=to_char(1234.34,'0000d00l');
v_char5:=to_char(-485,'99$9Mi');
v_char6:=to_char(23.12, '99.99BCMI');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun1 from dual;
--echo # 展示结果
select println() from dual;