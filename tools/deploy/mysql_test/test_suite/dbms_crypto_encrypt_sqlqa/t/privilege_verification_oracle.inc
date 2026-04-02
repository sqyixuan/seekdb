#owner:sean.yyj
#owner group: SQL-SQL3
#tags: privilege_verification_oracle
#description: utf8,dbms_crypto_hash_sqlqa,privilege_verification_oracle
#author: wb-sl672505

purge recyclebin;
let $char_charset = 'utf8';

--source mysql_test/include/charset/create_tenant_by_charset.inc
--source mysql_test/include/charset/conn_tenant.inc


###需要权限grant execute on dbms_crypto to user，默认DBA，有此权限;Grant execute on dbms_crypto to public;也行
###SET NAMES GBK;
###权限测试dbms_crypto.hash-4及其所有

grant DBA, resource,connect to public;
sleep 5;
revoke DBA, resource,connect from public;

--disable_warnings
--error 0,1919
drop role default_role_r1;
--error 0,1918,600
drop user default_role_u1 cascade;
--enable_warnings

create role default_role_r1;
create user default_role_u1 identified by default_role_u1;
grant create session, alter user to default_role_u1;

grant dba,connect,resource to default_role_u1 with admin option; 
sleep 5;
###revoke dba,resource from default_role_u1;
--source mysql_test/include/check_schema_sync.inc


--echo #########1.1.DBA用户测试##########
connect (default_role_u1,$OBMYSQL_MS0,default_role_u1@oracle,default_role_u1,*NO-ONE*,$OBMYSQL_PORT);
connection default_role_u1;
--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc

###sqlplus default_role_u1/default_role_u1@100.88.109.130:1522/orcl12c
###./obclient -h11.160.211.179 -P46010 -udefault_role_u1@oracle -pdefault_role_u1 -Ddefault_role_u1

delimiter //;

####ORACLE有报错 06550,201 ，OB全部没有限制，下同。
declare
   l_src_data varchar2(50):='33010319940513919X';
   l_hash raw(200);
begin
  l_hash := dbms_crypto.hash(
	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
	typ=>dbms_crypto.hash_md5);
	dbms_output.put_line(l_hash);
end;
//

delimiter ;//
select println() from dual;


--echo #########函数场景测试##########
delimiter //;

create or replace function funhash
(passid in varchar2
)
return varchar2
 is
v_varchar2 varchar2(200);
begin
v_varchar2 := dbms_crypto.hash(
	src=>utl_i18n.string_to_raw(passid,'AL32UTF8'),
	typ=>dbms_crypto.hash_md5)
	;
return v_varchar2;
end;
//

delimiter ;//
select funhash('33010319940513919X') from dual;
--echo # 展示结果
select println() from dual;

disconnect default_role_u1;
###ob已有的系统包都是没有权限控制的。dbms_crypto这个包也属于这个情况,现在不好改了，有兼容性问题。以下用例暂时注释
###--echo #########1.2.切换用户，回收DBA权限测试##########
###connection conn_charset_test_user;
###revoke dba,resource from default_role_u1;
###sleep 5;
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter //;
###
###--error 06550,201
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###disconnect default_role_u1;
###
###
###--echo #########1.3.再次赋予DBA权限测试##########
###connection conn_charset_test_user;
###grant dba,connect,resource to default_role_u1 with admin option; 
###sleep 5;
######revoke dba,resource from default_role_u1;
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
######delimiter ;//
######select funhash('33010319940513919X') from dual;
######--echo # 展示结果
######select println() from dual;
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###disconnect default_role_u1;
###
###
###--echo #########public权限测试##########
###connection conn_charset_test_user;
###
###grant DBA, resource,connect to public;
###sleep 5;
###
###--disable_warnings
###--error 0,1919
###drop role default_role_r1;
###--error 0,1918,600
###drop user default_role_u1 cascade;
###--enable_warnings
###
###create role default_role_r1;
###create user default_role_u1 identified by default_role_u1;
###grant create session, alter user to default_role_u1;
###
###--source mysql_test/include/check_schema_sync.inc
###
###
###--echo #########2.1.DBA用户测试##########
###connect (default_role_u1,$OBMYSQL_MS0,default_role_u1@oracle,default_role_u1,*NO-ONE*,$OBMYSQL_PORT);
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###
###--echo #########函数场景测试##########
###delimiter //;
###
###create or replace function funhash
###(passid in varchar2
###)
###return varchar2
### is
###v_varchar2 varchar2(200);
###begin
###v_varchar2 := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(passid,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5)
###	;
###return v_varchar2;
###end;
###//
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###disconnect default_role_u1;
###
###--echo #########2.2.切换用户，回收DBA权限测试##########
###connection conn_charset_test_user;
###revoke DBA, resource,connect from public;
###
###sleep 5;
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###--error 
###select funhash('33010319940513919X') from dual;
###
###
###delimiter //;
###
###--error 
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###disconnect default_role_u1;
###
###
###--echo #########2.3.再次赋予DBA权限测试##########
###connection conn_charset_test_user;
###grant DBA, resource,connect to public;
###sleep 5;
###
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###disconnect default_role_u1;
###
###
###--echo #########赋予sys包权限测试##########
###connection conn_charset_test_user;
###
###grant DBA, resource,connect to public;
###sleep 5;
###revoke DBA, resource,connect from public;
###
###--disable_warnings
###--error 0,1919
###drop role default_role_r1;
###--error 0,1918,600
###drop user default_role_u1 cascade;
###--enable_warnings
###
###create role default_role_r1;
###create user default_role_u1 identified by default_role_u1;
###grant create session, alter user to default_role_u1;
###
###grant dba,connect,resource to default_role_u1 with admin option; 
###sleep 5;
###revoke dba,resource from default_role_u1;
###grant execute on dbms_crypto to default_role_u1;
###sleep 5;
###--source mysql_test/include/check_schema_sync.inc
###
###
###
###--echo #########3.1.DBA用户测试##########
###connect (default_role_u1,$OBMYSQL_MS0,default_role_u1@oracle,default_role_u1,*NO-ONE*,$OBMYSQL_PORT);
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###
###--echo #########函数场景测试##########
###delimiter //;
###
###create or replace function funhash
###(passid in varchar2
###)
###return varchar2
### is
###v_varchar2 varchar2(200);
###begin
###v_varchar2 := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(passid,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5)
###	;
###return v_varchar2;
###end;
###//
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###disconnect default_role_u1;
###
###--echo #########3.2.切换用户，回收DBA权限测试##########
###connection conn_charset_test_user;
###revoke execute on dbms_crypto from default_role_u1;
###sleep 5;
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###--error 
###select funhash('33010319940513919X') from dual;
###
###
###delimiter //;
###
###--error 
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###disconnect default_role_u1;
###
###
###--echo #########3.3.再次赋予DBA权限测试##########
###connection conn_charset_test_user;
###grant execute on dbms_crypto to default_role_u1;
###sleep 5;
###
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###disconnect default_role_u1;
###
###
###
###
###--echo #########public权限测试##########
###connection conn_charset_test_user;
###
###grant resource,connect to public;
###grant execute on dbms_crypto to public;
###
###sleep 5;
###
###--disable_warnings
###--error 0,1919
###drop role default_role_r1;
###--error 0,1918,600
###drop user default_role_u1 cascade;
###--enable_warnings
###
###create role default_role_r1;
###create user default_role_u1 identified by default_role_u1;
###grant create session, alter user to default_role_u1;
###
###--source mysql_test/include/check_schema_sync.inc
###
###
###--echo #########4.1.DBA用户测试##########
###connect (default_role_u1,$OBMYSQL_MS0,default_role_u1@oracle,default_role_u1,*NO-ONE*,$OBMYSQL_PORT);
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###
###--echo #########函数场景测试##########
###delimiter //;
###
###create or replace function funhash
###(passid in varchar2
###)
###return varchar2
### is
###v_varchar2 varchar2(200);
###begin
###v_varchar2 := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(passid,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5)
###	;
###return v_varchar2;
###end;
###//
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###disconnect default_role_u1;
###
###--echo #########4.2.切换用户，回收DBA权限测试##########
###connection conn_charset_test_user;
###
###revoke execute on dbms_crypto from public;
###
###sleep 5;
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###--error 
###select funhash('33010319940513919X') from dual;
###
###
###delimiter //;
###
###--error 
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###disconnect default_role_u1;
###
###
###--echo #########4.3.再次赋予DBA权限测试##########
###connection conn_charset_test_user;
###grant execute on dbms_crypto to public;
###sleep 5;
###
###--source mysql_test/include/check_schema_sync.inc
###
###connection default_role_u1;
###--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_enable.inc
###
###delimiter ;//
###select funhash('33010319940513919X') from dual;
###--echo # 展示结果
###select println() from dual;
###
###delimiter //;
###
###declare
###   l_src_data varchar2(50):='33010319940513919X';
###   l_hash raw(200);
###begin
###  l_hash := dbms_crypto.hash(
###	src=>utl_i18n.string_to_raw(l_src_data,'AL32UTF8'),
###	typ=>dbms_crypto.hash_md5);
###	dbms_output.put_line(l_hash);
###end;
###//
###
###delimiter ;//
###select println() from dual;
###
###disconnect default_role_u1;


--source mysql_test/test_suite/dbms_crypto_hash_sqlqa/t/output_print_disable.inc
--echo  ########清空回收站，删除测试数据库，表等 ##########
--source mysql_test/include/charset/drop_tenant.inc
purge recyclebin;