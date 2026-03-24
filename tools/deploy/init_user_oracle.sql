alter session set current_schema = SYS;

create user root IDENTIFIED BY root;
grant all on *.* to root WITH GRANT OPTION;
grant dba to root;

create user test IDENTIFIED BY test;
grant all on *.* to test WITH GRANT OPTION;
grant dba to test;
grant all privileges to test;

create user admin IDENTIFIED BY admin;
grant all on *.* to admin WITH GRANT OPTION;
grant dba to admin;
grant all privileges to admin;

alter user LBACSYS account unlock;
alter user LBACSYS IDENTIFIED BY LBACSYS;
grant all on *.* to LBACSYS WITH GRANT OPTION;
grant dba to LBACSYS;

alter user ORAAUDITOR account unlock;
alter user ORAAUDITOR IDENTIFIED BY ORAAUDITOR;
grant all on *.* to ORAAUDITOR WITH GRANT OPTION;
grant dba to ORAAUDITOR;

create directory "DEFAULT" as '.';
grant read on directory "DEFAULT" to test;

ANALYZE TABLE SYS.ALL_VIRTUAL_CORE_ALL_TABLE COMPUTE STATISTICS;
ANALYZE TABLE SYS.ALL_VIRTUAL_CORE_COLUMN_TABLE COMPUTE STATISTICS;

