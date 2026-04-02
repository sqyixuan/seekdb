--error 0,00942
drop table tab01;
create table tab01(id number,c1 number) partition by hash(c1)partitions 4;
INSERT INTO tab01 SELECT 0,ROWNUM FROM DUAL CONNECT BY ROWNUM <= 10;

--error 0,00942
drop table tab02;
create table tab02(id number,c1 number) partition by hash(c1)partitions 4;
INSERT INTO tab02 SELECT 0,ROWNUM FROM DUAL CONNECT BY ROWNUM <= 10;

--error 0,00942
drop table tab_log;
create table tab_log(
  id number,
  username varchar2(50),
  eventime TIMESTAMP(6),
  action varchar2(100),
  Object varchar2(100))
partition by hash(id)partitions 4;
insert into tab_log values (0,'【用户名】',systimestamp,'【触发事件】','【触发对象】');

--error 0,00942
drop table tab_err;
create table tab_err(id number,msg varchar2(200));
insert into tab_err values (0,'【发生 EXCEPTION 信息】');

--error 0,00942
drop table tab_cunt;
create table tab_cunt(id number,c0 varchar2(50),c1 number);
insert into tab_cunt values (0,'【记录修改 c1 列后的值】',0);

--error 0,02289
DROP SEQUENCE seq1;
CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 MAXVALUE 200000 CYCLE NOCACHE;

--error 0,00600
drop package my_pack2;

delimiter /;
##删除所有触发器
CREATE OR REPLACE PROCEDURE delete_all_objects(a varchar2 default user) AS
  cnt number;
BEGIN
  select count(1) into cnt from all_triggers where TRIGGER_NAME like '%#_LOG%' ESCAPE '#' and OWNER = a;
  if cnt != 0 then
    FOR tri IN (select distinct OWNER,TRIGGER_NAME from all_triggers where TRIGGER_NAME like '%#_LOG%' ESCAPE '#' and OWNER = a) LOOP
      EXECUTE IMMEDIATE 'DROP TRIGGER ' || a || '.' || tri.TRIGGER_NAME;
    END LOOP;
  end if;
  DBMS_OUTPUT.PUT_LINE('drop all TRIGGER finish');
  COMMIT;
END;
/
#包变量
create or replace package my_pack2 is
  pack_cunt number default 1;
  function get_pack_cunt return number;
  function set_pack_cunt(p1 number default 1) return number;
end;
/
create or replace package body my_pack2 is
  function get_pack_cunt return number is
  begin
    return pack_cunt;
  end;
  function set_pack_cunt(p1 number default 1) return number is
  begin
    pack_cunt := p1;
    return pack_cunt;
  end;
end;
/
delimiter ;/
