#owner: ailing.lcq
#owner group: sql-执行器
#description: include文件
#tags: prepare table
#author:wb-sl672505

connect (sys1,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/drop_tenant1.inc
connect (sys2,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/drop_tenant2.inc
##--source mysql_test/test_suite/otimestamp/t/otimestamp_import_time_zone.inc

connection default;
--error 0,600
drop user yyl_dblink cascade;
create user yyl_dblink IDENTIFIED BY 123;
grant all on *.* to yyl_dblink WITH GRANT OPTION;
grant dba to yyl_dblink;

--source mysql_test/include/check_schema_sync.inc
connect (conn,$OBMYSQL_MS0,yyl_dblink@oracle,123,yyl_dblink,$OBMYSQL_PORT);
connection conn;

--disable_warnings
--error 0,2024
DROP DATABASE link my_link11;
--error 0,2024
DROP DATABASE link my_link12;
--error 0,942
DROP TABLE test301 CASCADE CONSTRAINTS purge;
--enable_warnings
purge recyclebin;

let $char_charset = 'gb18030';
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/create_tenant_by_charset1.inc
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/conn_tenant1.inc
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/create_tenant_by_charset2.inc
--source mysql_test/test_suite/dblink_oboracle_oracle_selectall_sqlqa/include/conn_tenant2.inc
connection conn;

--disable_query_log
eval create database link my_link11 connect to 'yyl_dblink'@dblink_tnt1 identified by 123 host '$OBMYSQL_MS0:$OBMYSQL_PORT';
eval create database link my_link12 connect to 'yyl_dblink'@dblink_tnt2 identified by 123 host '$OBMYSQL_MS0:$OBMYSQL_PORT';
--enable_query_log

--echo #dblink	远端表类型
--echo #dblink	表类型
connection conn_dblink_tnt1;
--echo #dblink	普通表

--disable_warnings
--error 0,942
drop table EMP;
--error 0,942
drop table DEPT;
--enable_warnings

CREATE TABLE DEPT(DEPTNO NUMBER(2),DNAME VARCHAR2(14),LOC VARCHAR2(13));
CREATE TABLE EMP(EMPNO NUMBER(4) ,ENAME VARCHAR2(10),JOB VARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);


--disable_warnings
--error 0,942
drop table olympic_medal_winners;
--enable_warnings

create table olympic_medal_winners(   
  olympic_year int,  
  sport        varchar2(30),  
  gender       varchar2(1),  
  event        varchar2(128),  
  medal        varchar2(10),  
  noc          varchar2(3),  
  athlete      varchar2(128) 
);

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Gold','KOR','KU Bonchan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Silver','FRA','VALLADONT Jean-Charles');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Bronze','USA','ELLISON Brady');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Gold','KOR','Republic of Korea');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Bronze','AUS','Australia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Silver','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Gold','GBR','WHITLOCK Max');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Bronze','BRA','MARIANO Arthur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Silver','BRA','HYPOLITO Diego');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Gold','GER','HAMBUECHEN Fabian');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Bronze','GBR','WILSON Nile');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Silver','USA','LEYVA Danell');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Gold','GBR','FARAH Mohamed');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Bronze','ETH','TOLA Tamirat');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Silver','KEN','TANUI Paul Kipngetich');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Gold','JAM','BOLT Usain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Silver','USA','GATLIN Justin');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Bronze','CAN','DE GRASSE Andre');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Gold','CHN','Zhang');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Bronze','GBR','Langridge');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Bronze','GBR','Ellis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Silver','MAS','Tan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Silver','MAS','Goh');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Gold','CHN','Fu');

select count(*) from olympic_medal_winners;

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Gold','BRA','Cerutti');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Gold','BRA','Oscar Schmidt');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Silver','ITA','Nicolai');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Silver','ITA','Lupo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Bronze','NED','Meeuwsen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Bronze','NED','Brouwer');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Gold','CUB','RAMIREZ Robeisy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Bronze','UZB','AKHMADALIEV Murodjon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Bronze','RUS','NIKITIN Vladimir');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Silver','USA','STEVENSON Shakur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Fly (52kg)','Gold','UZB','ZOIROV Shakhobidin');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Fly (52kg)','Bronze','CHN','HU Jianguan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Gold','SVK','PETER Skantar');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Bronze','FRA','GAUTHIER Klauss');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Bronze','FRA','MATTHIEU Peche');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Silver','GBR','RICHARD Hounslow');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Silver','GBR','DAVID Florence');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Gold','SVK','LADISLAV Skantar');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Gold','GER','Brendel');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Bronze','UKR','Mishchuk');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Bronze','UKR','Ianchuk');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Silver','BRA','Queiroz dos Santos');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Silver','BRA','de Souza Silva');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Gold','GER','Vandrey');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Gold','SUI','CANCELLARA Fabian');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Bronze','GBR','FROOME Christopher');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Silver','NED','DUMOULIN Tom');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Gold','BEL','VAN AVERMAET Greg');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Silver','DEN','FUGLSANG Jakob');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Bronze','POL','MAJKA Rafal');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Gold','GBR','KENNY Jason');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Bronze','MAS','AWANG Azizulhasni');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Silver','NED','BUCHLI Matthijs');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Gold','ITA','VIVIANI Elia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Bronze','DEN','HANSEN Lasse Norman');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Silver','GBR','CAVENDISH Mark');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Gold','CHN','CHEN Aisen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Bronze','USA','BOUDIA David');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Silver','MEX','SANCHEZ German');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Gold','CHN','CAO Yuan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Silver','GBR','LAUGHER Jack');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Bronze','GER','HAUSDING Patrick');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Gold','GBR','DUJARDIN Charlotte');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Bronze','GER','BRORING-SPREHE Kristina');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Silver','GER','WERTH Isabell');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Gold','GER','Germany');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Silver','GBR','Great Britain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Gold','ITA','GAROZZO Daniele');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Silver','USA','MASSIALAS Alexander');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Bronze','RUS','SAFIN Timur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Gold','DEN','Denmark');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Bronze','NOR','Norway');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Gold','ARG','Argentina');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Silver','BEL','Belgium');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Gold','GBR','Great Britain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Silver','NED','Netherlands');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Gold','FRA','RINER Teddy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Bronze','BRA','SILVA Rafael');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Bronze','ISR','SASSON Or');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Silver','JPN','HARASAWA Hisayoshi');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men -100 kg','Gold','CZE','KRPALEK Lukas');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men -100 kg','Bronze','FRA','MARET Cyrille');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Gold','RUS','LESUN Alexander');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Silver','UKR','TYMOSHCHENKO Pavlo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Bronze','MEX','HERNANDEZ USCANGA Ismael Marcelo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Gold','AUS','ESPOSITO Chloe');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Silver','FRA','CLOUVEL Elodie');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Bronze','POL','NOWACKA Oktawia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Bronze','BUL','Bulgaria');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Silver','ESP','Spain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Gold','RUS','MAMUN Margarita');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Silver','RUS','KUDRYAVTSEVA Yana');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Bronze','UKR','RIZATDINOVA Ganna');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Gold','FRA','Azou');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Bronze','NOR','Brun');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Bronze','NOR','Strandli');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Silver','IRL','O''Donovan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Silver','IRL','O''Donovan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Gold','FRA','Houin');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Gold','CRO','Fantela');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Bronze','GRE','Kagialis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Bronze','GRE','Mantis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Silver','AUS','Ryan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Silver','AUS','Belcher');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Gold','CRO','Marenic');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Gold','USA','MURPHY Ryan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Bronze','USA','PLUMMER David');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Silver','CHN','XU Jiayu');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Gold','GBR','PEATY Adam');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Bronze','USA','MILLER Cody');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Silver','RSA','VAN DER BURGH Cameron');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Gold','CHN','MA Long');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Bronze','JPN','MIZUTANI Jun');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Silver','CHN','ZHANG Jike');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Gold','CHN','China');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Silver','JPN','Japan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Gold','AZE','ISAEV Radik');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Bronze','KOR','CHA Dongmin') ;
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Bronze','BRA','SIQUEIRA Maicon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Silver','NIG','ISSOUFOU ALFAGA Abdoulrazak');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men -58kg','Gold','CHN','ZHAO Shuai');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men -58kg','Silver','THA','HANPRAB Tawin');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Gold','ESP','Lopez');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Bronze','USA','Johnson');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Bronze','USA','Sock');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Silver','ROU','Tecau');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Silver','ROU','Mergea');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Gold','ESP','Nadal');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Gold','BLR','HANCHAROU Uladzislau');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Silver','CHN','DONG Dong');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Bronze','CHN','GAO Lei');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Gold','CAN','MACLENNAN Rosannagh');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Silver','GBR','PAGE Bryony');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Bronze','CHN','LI Dan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Gold','GBR','BROWNLEE Alistair');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Silver','GBR','BROWNLEE Jonathan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Bronze','RSA','SCHOEMAN Henri');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Gold','USA','JORGENSEN Gwen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Silver','SUI','SPIRIG HUG Nicola');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Bronze','GBR','HOLLAND Vicky');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Gold','BRA','Brazil');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Silver','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Gold','CHN','China');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Silver','SRB','Serbia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Gold','SRB','Serbia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Silver','CRO','Croatia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Bronze','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Gold','USA','United States');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Silver','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Bronze','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Gold','GEO','TALAKHADZE Lasha');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Bronze','GEO','TURMANIDZE Irakli');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Silver','ARM','MINASYAN Gor');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Gold','UZB','NURUDINOV Ruslan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Bronze','KAZ','ZAICHIKOV Alexandr');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Silver','ARM','MARTIROSYAN Simon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Gold','TUR','AKGUL Taha');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Bronze','BLR','SAIDAU Ibrahim');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Bronze','GEO','PETRIASHVILI Geno');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Silver','IRI','GHASEMI Komeil Nemat');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 57 kg','Gold','GEO','KHINCHEGASHVILI Vladimer');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 57 kg','Bronze','AZE','ALIYEV Haji');



--error 0,942
DROP TABLE test100 CASCADE CONSTRAINTS purge;
CREATE TABLE test100(f1 int, f2 varchar(10), f3 varchar(10));
INSERT INTO test100 VALUES(1001,'a','12345');
INSERT INTO test100 VALUES(1002,'b','67890');
INSERT INTO test100 VALUES(1003,'c','23456');
commit;

--error 0,942
DROP TABLE test101 purge;
--echo #range分区
CREATE TABLE test101(f1 NUMBER(5,0) PRIMARY KEY, f2 nvarchar2(10), f3 varchar(10)) 
PARTITION BY RANGE(f1)(PARTITION p1 VALUES less than(1003),PARTITION p2 VALUES less than(1005),PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO test101 VALUES(1001,'a','12345');
INSERT INTO test101 VALUES(1002,'b','67890');
INSERT INTO test101 VALUES(1003,'c','23456');
INSERT INTO test101 VALUES(1006,'d','23456');
--echo #hash分区
--error 0,942
DROP TABLE test102 purge;
CREATE TABLE test102(f1 NUMBER(5,0) PRIMARY KEY, f2 nvarchar2(10), f3 varchar(10)) 
PARTITION BY hash(f1) PARTITIONS 3;
INSERT INTO test102 VALUES(1001,'a','12345');
INSERT INTO test102 VALUES(1003,'b','67890');
INSERT INTO test102 VALUES(1004,'c','23456');
INSERT INTO test102 VALUES(1006,'d','23456');
--echo #同构二级分区（hash-range）
--error 0,942
DROP TABLE test103 purge;
CREATE TABLE test103(f1 NUMBER(5,0), f2 nvarchar2(10), f3 varchar(10),PRIMARY KEY(f1,f2)) 
PARTITION BY hash(f2) subpartition BY RANGE(f1)
subpartition template (subpartition sub1 VALUES less than (1004),subpartition sub2 VALUES less than (MAXVALUE)) PARTITIONS 2;
INSERT INTO test103 VALUES(1001,'a','12345');
INSERT INTO test103 VALUES(1003,'b','67890');
INSERT INTO test103 VALUES(1004,'c','23456');
INSERT INTO test103 VALUES(1006,'d','23456');
INSERT INTO test103 VALUES(1007,'e','23456');
commit;

--error 0,942
DROP TABLE test104 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test105 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE testest106 CASCADE CONSTRAINTS purge;

CREATE TABLE test104(f1 int PRIMARY KEY, f2 nvarchar2(10), f3 varchar(10));
INSERT INTO test104 VALUES(1001,'a','12345');
INSERT INTO test104 VALUES(1003,'b','67890');
INSERT INTO test104 VALUES(1004,'c','23456');
INSERT INTO test104 VALUES(1006,'d','23456');
INSERT INTO test104 VALUES(1007,'e','23456');
--echo #basic压缩
CREATE TABLE test105 COMPRESS basic AS SELECT * FROM test104;
###--echo #for query压缩
###CREATE TABLE testest106 COMPRESS FOR query low AS SELECT * FROM test104;
commit;

--error 0,942
DROP TABLE test106 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test107 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test108 CASCADE CONSTRAINTS purge;
CREATE TABLE test106(f1 NUMBER PRIMARY KEY);
INSERT INTO test106 VALUES(1e10);
INSERT INTO test106 VALUES(1e-10);
INSERT INTO test106 VALUES(5);
CREATE TABLE test107(f1 NUMBER(15));
INSERT INTO test107 VALUES(1e10);
INSERT INTO test107 VALUES(1e-10);
INSERT INTO test107 VALUES(5);
CREATE TABLE test108(f1 NUMBER(15,12));
INSERT INTO test108 VALUES(100);
INSERT INTO test108 VALUES(1e-10);
INSERT INTO test108 VALUES(5.5);


--error 0,942
DROP TABLE test109 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test110 CASCADE CONSTRAINTS purge;
CREATE TABLE test109(f1 float);
INSERT INTO test109 VALUES(1e10);
INSERT INTO test109 VALUES(1e-10);
INSERT INTO test109 VALUES(5);
CREATE TABLE test110(f1 float(15) PRIMARY KEY);
INSERT INTO test110 VALUES(1e10);
INSERT INTO test110 VALUES(1e-10);
INSERT INTO test110 VALUES(5);

--error 0,942
DROP TABLE test111 CASCADE CONSTRAINTS purge;
CREATE TABLE test111(f1 binary_float);
INSERT INTO test111 VALUES(1e10);
INSERT INTO test111 VALUES(1e-10);
INSERT INTO test111 VALUES(5);

--error 0,942
DROP TABLE test112 CASCADE CONSTRAINTS purge;
CREATE TABLE test112(f1 binary_double PRIMARY KEY);
INSERT INTO test112 VALUES(1e10);
INSERT INTO test112 VALUES(1e-10);
INSERT INTO test112 VALUES(5);

--error 0,942
DROP TABLE test113 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test114 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test115 CASCADE CONSTRAINTS purge;

CREATE TABLE test113(f1 varchar2(5)) PARTITION BY hash(f1) PARTITIONS 2;
INSERT INTO test113 VALUES('11');
INSERT INTO test113 VALUES('aa');
INSERT INTO test113 VALUES('33');
CREATE TABLE test114(f1 varchar2(5 byte) PRIMARY KEY);
INSERT INTO test114 VALUES('11');
INSERT INTO test114 VALUES('aa');
INSERT INTO test114 VALUES('66');
###--error 0,900
###CREATE TABLE test115(f1 varchar2(5 varchar2) PRIMARY KEY);
###INSERT INTO test115 VALUES('111');
###INSERT INTO test115 VALUES('aaa');
###INSERT INTO test115 VALUES('666');

--error 0,942
DROP TABLE test116 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test117 CASCADE CONSTRAINTS purge;

###ob创建表时，暂不支持创建（未显式指定size的nchar类型）的字段，oracle支持
CREATE TABLE test116(f1 nvarchar2(20) PRIMARY KEY);
INSERT INTO test116 VALUES('1');
INSERT INTO test116 VALUES('a');
INSERT INTO test116 VALUES('3');
CREATE TABLE test117(f1 nvarchar2(20) PRIMARY KEY) COMPRESS FOR oltp PARTITION BY hash(f1) PARTITIONS 2;
INSERT INTO test117 VALUES('11');
INSERT INTO test117 VALUES('aa');
INSERT INTO test117 VALUES('66');

--error 0,942
DROP TABLE test118 CASCADE CONSTRAINTS purge;

CREATE TABLE test118(f1 nvarchar2(5) PRIMARY KEY) COMPRESS FOR oltp;
INSERT INTO test118 VALUES('11');
INSERT INTO test118 VALUES('aa');
INSERT INTO test118 VALUES('66');

--error 0,942
DROP TABLE test119 CASCADE CONSTRAINTS purge;
CREATE TABLE test119(f1 int,f2 DATE) PARTITION BY RANGE(f2) (PARTITION p1 VALUES less than (to_date('25-07-2008 12:01:00','dd-mm-yyyy hh24:mi:ss')), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test119 VALUES(101,to_date('25-09-2005 12:01:00','dd-mm-yyyy hh24:mi:ss'));
INSERT INTO test119 VALUES(103,to_date('25-09-2009 12:01:00','dd-mm-yyyy hh24:mi:ss'));
INSERT INTO test119 VALUES(105,to_date('25-09-2020 12:01:00','dd-mm-yyyy hh24:mi:ss'));

--error 0,942
DROP TABLE test120 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test121 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test122 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test123 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test124 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test125 CASCADE CONSTRAINTS purge;

CREATE TABLE test120(f1 int,f2 timestamp);
INSERT INTO test120 VALUES(101,to_timestamp('25-09-2005 12:01:09.9999','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test120 VALUES(103,to_timestamp('25-09-2009 12:01:05.66666','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test120 VALUES(105,to_timestamp('25-09-2020 12:01:03.55555','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test121(f1 int,f2 timestamp(5),CONSTRAINT pk2 PRIMARY KEY(f2));
INSERT INTO test121 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test121 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test121 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test122(f1 int,f2 timestamp WITH time ZONE);
INSERT INTO test122 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test122 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test122 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test123(f1 int,f2 timestamp(5) WITH time ZONE,CONSTRAINT pk4 PRIMARY KEY(f1));
INSERT INTO test123 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test123 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test123 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test124(f1 int,f2 timestamp WITH LOCAL time ZONE);
INSERT INTO test124 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test124 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test124 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test125(f1 int,f2 timestamp(5) WITH LOCAL time ZONE,CONSTRAINT pk6 PRIMARY KEY(f2));
INSERT INTO test125 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test125 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test125 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));

--error 0,942
DROP TABLE test126 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test127 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test128 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test129 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test130 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test131 CASCADE CONSTRAINTS purge;
CREATE TABLE test126(f1 int,f2 INTERVAL YEAR TO MONTH);
INSERT INTO test126 VALUES(101,numtoyminterval(1.5,'year')+numtoyminterval(1,'month'));
INSERT INTO test126 VALUES(103,numtoyminterval(1,'year')+numtoyminterval(2,'month'));
INSERT INTO test126 VALUES(105,numtoyminterval(1,'year')+numtoyminterval(3,'month'));
CREATE TABLE test127(f1 int,f2 INTERVAL YEAR(5) TO MONTH,CONSTRAINT pk22 PRIMARY KEY(f1,f2));
INSERT INTO test127 VALUES(101,numtoyminterval(1.5,'year')+numtoyminterval(1,'month'));
INSERT INTO test127 VALUES(103,numtoyminterval(2,'year')+numtoyminterval(1,'month'));
INSERT INTO test127 VALUES(105,numtoyminterval(3,'year')+numtoyminterval(1,'month'));
CREATE TABLE test128(f1 int,f2 INTERVAL DAY TO SECOND);
INSERT INTO test128 VALUES(101,numtodsinterval(1,'day')+numtodsinterval(2,'hour')+numtodsinterval(3,'minute')+numtodsinterval(4,'second'));
INSERT INTO test128 VALUES(102,numtodsinterval(1.5,'day')+numtodsinterval(2.5,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(4.5,'second'));
INSERT INTO test128 VALUES(106,numtodsinterval(2.5,'day')+numtodsinterval(2,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(5.5,'second'));
CREATE TABLE test129(f1 int,f2 INTERVAL DAY(4) TO SECOND,CONSTRAINT pk44 PRIMARY KEY(f2));
INSERT INTO test129 VALUES(101,numtodsinterval(1,'day')+numtodsinterval(2,'hour')+numtodsinterval(3,'minute')+numtodsinterval(4,'second'));
INSERT INTO test129 VALUES(102,numtodsinterval(1.5,'day')+numtodsinterval(2.5,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(4.5,'second'));
INSERT INTO test129 VALUES(106,numtodsinterval(2.5,'day')+numtodsinterval(2,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(5.5,'second'));
CREATE TABLE test130(f1 int,f2 INTERVAL DAY TO SECOND(5),CONSTRAINT pk55 PRIMARY KEY(f2));
INSERT INTO test130 VALUES(101,numtodsinterval(1,'day')+numtodsinterval(2,'hour')+numtodsinterval(3,'minute')+numtodsinterval(4,'second'));
INSERT INTO test130 VALUES(102,numtodsinterval(1.5,'day')+numtodsinterval(2.5,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(4.5,'second'));
INSERT INTO test130 VALUES(106,numtodsinterval(2.5,'day')+numtodsinterval(2,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(5.5,'second'));
CREATE TABLE test131(f1 int,f2 INTERVAL DAY(4) TO SECOND(5));
INSERT INTO test131 VALUES(101,numtodsinterval(1,'day')+numtodsinterval(2,'hour')+numtodsinterval(3,'minute')+numtodsinterval(4,'second'));
INSERT INTO test131 VALUES(102,numtodsinterval(1.5,'day')+numtodsinterval(2.5,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(4.5,'second'));
INSERT INTO test131 VALUES(106,numtodsinterval(2.5,'day')+numtodsinterval(2,'hour')+numtodsinterval(3.5,'minute')+numtodsinterval(5.5,'second'));

###--error 0,942
###DROP TABLE test132 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test132(f1 int PRIMARY KEY,f2 LONG);
###INSERT INTO test132 VALUES(123,'adminadminadmin');
###INSERT INTO test132 VALUES(456,'testtesttest');

--error 0,942
DROP TABLE test133 CASCADE CONSTRAINTS purge;
CREATE TABLE test133(f1 int,f2 raw(20)) COMPRESS FOR oltp;
INSERT INTO test133 VALUES(101,utl_raw.cast_to_raw('rawtype'));
INSERT INTO test133 VALUES(102,utl_raw.cast_to_raw('1234567'));

###--error 0,942
###DROP TABLE test134 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test134(f1 int PRIMARY KEY,f2 long raw);
###INSERT INTO test134 VALUES(101,utl_raw.cast_to_raw('rawtype'));
###INSERT INTO test134 VALUES(103,utl_raw.cast_to_raw('1234567'));

###--error 0,942
###DROP TABLE test135 CASCADE CONSTRAINTS purge;
###--error 0,942
###DROP TABLE test136 CASCADE CONSTRAINTS purge;
###--error 0,942
###DROP TABLE test137 CASCADE CONSTRAINTS purge;
###
###CREATE TABLE test135(f1 int,f2 rowid PRIMARY KEY);
###INSERT INTO test135 VALUES(1,'AAKAdoAAEAADFlAAAA');
###INSERT INTO test135 VALUES(2,'AAKAdoAAEAADFlAAAD');
###CREATE TABLE test136(f1 int,f2 urowid);
###INSERT INTO test136 VALUES(1,'AAKAdoAAEAADFlAAAB');
###INSERT INTO test136 VALUES(2,'AAKAdoAAEAADFlAAAd');
###CREATE TABLE test137(f1 int,f2 urowid(100) PRIMARY KEY);
###INSERT INTO test137 VALUES(1,'AAKAdoAAEAADFlAAAc');
###INSERT INTO test137 VALUES(2,'AAKAdoAAEAADFlAAAe');

--error 0,942
DROP TABLE test138 CASCADE CONSTRAINTS purge;

CREATE TABLE test138(f1 int PRIMARY KEY,f2 clob);
INSERT INTO test138 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test138 VALUES(2,'fal;jg;agopra/;.''po[ragp');
INSERT INTO test138 VALUES(4,'1234ddddddddddddddddddddddddddd');
INSERT INTO test138 VALUES(6,'fal;jg;agopra/;.''po[ragp');

--error 0,942
DROP TABLE test139 CASCADE CONSTRAINTS purge;
CREATE TABLE test139(f1 int,f2 blob);
INSERT INTO test139 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test139 VALUES(2,'41895fdabccccccccffff87897686aa');
INSERT INTO test139 VALUES(4,'1234ddddddddddddddddddddddddddd');
INSERT INTO test139 VALUES(6,'41895fdabccccccccffff87897686aa');

###--error 0,942
###DROP TABLE test140 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test140(f1 int PRIMARY KEY,f2 nclob);
###INSERT INTO test140 VALUES(1,'AAJDTjAAEAAITskAAA');
###INSERT INTO test140 VALUES(2,'AAJDTnAAEAAITtcAAB');

--error 0,942
DROP TABLE test141 CASCADE CONSTRAINTS purge;
CREATE TABLE test141(f1 NUMBER(10), f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9));
INSERT INTO test141 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test141 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test141 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test141 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

--error 0,942
DROP TABLE test142 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test143 CASCADE CONSTRAINTS purge;
CREATE TABLE test142(f1 int PRIMARY KEY);
INSERT INTO test142 VALUES(1001);
INSERT INTO test142 VALUES(1002);
INSERT INTO test142 VALUES(1003);
CREATE TABLE test143(
	f1 NUMBER(10) PRIMARY KEY, 
	f2 binary_float UNIQUE, 
	f3 varchar2(20) NULL, 
	f4 nvarchar2(20) NOT NULL, 
	f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
	f6 int,
	CONSTRAINT fk_11 FOREIGN KEY(f6) REFERENCES test142(f1),
	CONSTRAINT check11 CHECK (f2 BETWEEN power(10,2) AND power(10,3)))
PARTITION BY RANGE(f1) 
	(PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (100000));
INSERT INTO test143 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test143 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test143 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test143 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);

--error 0,942
DROP TABLE test144 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test145 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test146 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test147 CASCADE CONSTRAINTS purge;

CREATE TABLE test144(f1 int PRIMARY KEY);
INSERT INTO test144 VALUES(1001);
INSERT INTO test144 VALUES(1002);
INSERT INTO test144 VALUES(1003);

CREATE TABLE test145(f1 NUMBER(10), f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9));
CREATE INDEX index_1 ON test145(f2);
INSERT INTO test145 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test145 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test145 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test145 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

CREATE TABLE test146(f1 NUMBER(10) PRIMARY KEY, f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9)) PARTITION BY hash(f1) PARTITIONS 2;
CREATE INDEX index_2 ON test146(f2) LOCAL;
INSERT INTO test146 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test146 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test146 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test146 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

CREATE TABLE test147(
	f1 NUMBER(10) PRIMARY KEY, 
	f2 binary_float UNIQUE, 
	f3 varchar2(20) NULL, 
	f4 nvarchar2(20) NOT NULL, 
	f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
	f6 int,
	CONSTRAINT fk_2 FOREIGN KEY(f6) REFERENCES test144(f1),
	CONSTRAINT check3 CHECK (f2 BETWEEN power(10,2) AND power(10,3)))
PARTITION BY RANGE(f1) 
	(PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (maxvalue));
CREATE INDEX index_3 ON test147(f4) GLOBAL;
INSERT INTO test147 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test147 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test147 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test147 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);

--error 0,942
DROP TABLE test148 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test149 CASCADE CONSTRAINTS purge;

CREATE TABLE test148(f1 NUMBER(10,5), f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9));
CREATE INDEX index_111 ON test148(f2);
INSERT INTO test148 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test148 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test148 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test148 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));
CREATE TABLE test149(f1 NUMBER, f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9), f6 clob, f7 blob);
INSERT INTO test149 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'), '1234dddddddddd', '1234ddddddddddddddddd');
INSERT INTO test149 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'), 'fal;jg;agopra/;.''po[ragp', '41895fdabccff87897686aa');
INSERT INTO test149 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'), '1234dddddddddd', '1234ddddddddddddddddd');
INSERT INTO test149 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'), 'fal;jg;agopra/;.''po[ragp', '41895fdabccff87897686aa');

--error 0,942
DROP TABLE class150 CASCADE CONSTRAINTS purge;

CREATE TABLE class150 (id int, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
COMPRESS basic PARTITION BY list(id) (PARTITION p1 VALUES(101,102),PARTITION p2 VALUES(DEFAULT));
CREATE INDEX index_1150 ON class150(create_time);
INSERT INTO class150 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

--error 0,942
DROP TABLE stu150 CASCADE CONSTRAINTS purge;

CREATE TABLE stu150 (id int PRIMARY KEY, p_id int,name nvarchar2(50) NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int,
	CONSTRAINT check150 CHECK (weight BETWEEN 10 AND 100))
COMPRESS FOR oltp PARTITION BY RANGE(id) 
	(PARTITION p1 VALUES less than (1004),PARTITION p2 VALUES less than (maxvalue));
CREATE INDEX index_1250 ON stu150(birth) LOCAL;
INSERT INTO stu150 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu150 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu150 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu150 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu150 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu150 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu150 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--enable_warnings

create table gbktable
(
c0 int,
c1 nchar(200),
c2 nchar(200),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200),
c9 number(10,3)
);

insert into gbktable values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.2);
insert into gbktable values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee',100);
insert into gbktable values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff',100.3);
insert into gbktable values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc',200);
insert into gbktable values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.22);
insert into gbktable values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',3000);
insert into gbktable values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',2000.33);
insert into gbktable values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',1000);
insert into gbktable values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200',10000);
insert into gbktable values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300',999999);
insert into gbktable values(11,'中文测试011','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);

--disable_warnings
--error 0,942
DROP TABLE test151 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test152 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test153 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test154 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test151 (a int);
create table test152 (a int, b int);
create table test153 (a int);
create table test154 (a int not null, b int not null);
insert into test151 values (2);
insert into test152 values (1,7);
insert into test152 values (2,7);
insert into test153 values (6);
insert into test153 values (7);
insert into test153 values (3);
insert into test154 values (4,8);
insert into test154 values (3,8);
insert into test154 values (5,9);

connection conn_dblink_tnt1;
--echo #dblink  
--disable_warnings
--error 0,942
DROP TABLE test161 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test162 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test163 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test164 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test165 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test166 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test167 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test168 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test169 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test170 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test171 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test172 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test173 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test174 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test175 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test176 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test177 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test161 (a int, b int);
create table test162 (a int, b int);
create table test163 (a int, b int);
insert into test161 values (0,100);
insert into test161 values (1,2);
insert into test161 values (1,3);
insert into test161 values (2,2);
insert into test161 values (2,7);
insert into test161 values (2,-1);
insert into test161 values (3,10);
insert into test162 values (0,0);
insert into test162 values (1,1);
insert into test162 values (2,1);
insert into test162 values (3,1);
insert into test162 values (4,1);
insert into test163 values (3,3);
insert into test163 values (2,2);
insert into test163 values (1,1);

create table test164 (s1 int);
create table test165 (s1 int);
insert into test164 values (1);
insert into test165 values (1);

CREATE TABLE test166 (id int default NULL,name varchar2(20) default NULL);
INSERT INTO test166 VALUES (1,'Tim');
INSERT INTO test166 VALUES (2,'Rebecca');
INSERT INTO test166 VALUES (3,NULL);
CREATE TABLE test167 (id int default NULL, pet varchar2(20) default NULL);
INSERT INTO test167 VALUES (1,'Fido');
INSERT INTO test167 VALUES (2,'Spot');
INSERT INTO test167 VALUES (3,'Felix');

create table test168 (i int, j number(10));
insert into test168 values (1, 2);
insert into test168 values (2, 2);
insert into test168 values (3, 2);

CREATE TABLE test169 (a int, b INT, d INT, c VARCHAR2(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO test169 VALUES (1,1,0,'a');
INSERT INTO test169 VALUES (1,2,0,'b');
INSERT INTO test169 VALUES (1,3,0,'c');
INSERT INTO test169 VALUES (1,4,0,'d');
INSERT INTO test169 VALUES (1,5,0,'e');
INSERT INTO test169 VALUES (2,1,0,'f');
INSERT INTO test169 VALUES (2,2,0,'g');
INSERT INTO test169 VALUES (2,3,0,'h');
INSERT INTO test169 VALUES (3,4,0,'i');
INSERT INTO test169 VALUES (3,3,0,'j');
INSERT INTO test169 VALUES (3,2,0,'k');
INSERT INTO test169 VALUES (3,1,0,'l');
INSERT INTO test169 VALUES (1,9,0,'m');
INSERT INTO test169 VALUES (1,0,10,'n');
INSERT INTO test169 VALUES (2,0,5,'o');
INSERT INTO test169 VALUES (3,0,7,'p');

CREATE TABLE test170 (id int NOT NULL, st VARCHAR2(2));
INSERT INTO test170 VALUES (3,'FL');
INSERT INTO test170 VALUES (2,'GA');
INSERT INTO test170 VALUES (4,'FL');
INSERT INTO test170 VALUES (1,'GA');
INSERT INTO test170 VALUES (5,'NY');
INSERT INTO test170 VALUES (7,'FL');
INSERT INTO test170 VALUES (6,'NY');


CREATE TABLE test171 (id int NOT NULL);
INSERT INTO test171 VALUES (7);
INSERT INTO test171 VALUES (5);
INSERT INTO test171 VALUES (1);
INSERT INTO test171 VALUES (3);

create table test172 (a int, b int);
insert into test172 values (0,0);
insert into test172 values (2,2);
insert into test172 values (3,3);

create table test173 (a int, b int);
insert into test173 values (1,1);
insert into test173 values (3,3);
insert into test173 values (NULL,4);

CREATE TABLE test174 (
  pk INT,
  col_varchar VARCHAR2(1),
  PRIMARY KEY (pk)
)
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(1,'w')
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(2,'m')
;

CREATE TABLE test175 (c1 VARCHAR2(1), c2 INT, c3 INT);
INSERT/*insert trace*/ INTO test175 VALUES ('',6,8);
INSERT/*insert trace*/ INTO test175 VALUES ('',75,NULL);

CREATE TABLE test176 (c1 VARCHAR2(1));
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');

CREATE TABLE test177 (i1 INT);
INSERT/*insert trace*/ INTO test177 VALUES (2);
INSERT/*insert trace*/ INTO test177 VALUES (4);
INSERT/*insert trace*/ INTO test177 VALUES (6);
INSERT/*insert trace*/ INTO test177 VALUES (8);

--echo #dblink  
--disable_warnings
--error 0,942
DROP TABLE test181 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test182 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test183 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test184 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test185 CASCADE CONSTRAINTS purge;
--enable_warnings


CREATE TABLE test181 (
  categoryId int NOT NULL,
  courseId int NOT NULL,
  startDate date NOT NULL,
  endDate date NOT NULL,
  createDate date NOT NULL,
  modifyDate date NOT NULL,
  attributes varchar2(30)
);

INSERT INTO test181 VALUES (1,41,to_date('2004-02-09'),to_date('2010-01-01'),to_date('2004-02-09'),to_date('2004-02-09'),'');
INSERT INTO test181 VALUES (1,86,to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),'');
INSERT INTO test181 VALUES (1,87,to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),'');
INSERT INTO test181 VALUES (2,52,to_date('2004-03-15'),to_date('2004-10-01'),to_date('2004-03-15'),to_date('2004-09-17'),'');
INSERT INTO test181 VALUES (2,53,to_date('2004-03-16'),to_date('2004-10-01'),to_date('2004-03-16'),to_date('2004-09-17'),'');
INSERT INTO test181 VALUES (2,88,to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),'');
INSERT INTO test181 VALUES (2,89,to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),to_date('2004-08-16'),'');
INSERT INTO test181 VALUES (3,51,to_date('2004-02-09'),to_date('2010-01-01'),to_date('2004-02-09'),to_date('2004-02-09'),'');
INSERT INTO test181 VALUES (5,12,to_date('2004-02-18'),to_date('2010-01-01'),to_date('2004-02-18'),to_date('2004-02-18'),'');

CREATE TABLE test182 (
  userId int NOT NULL,
  courseId int NOT NULL,
  datett date NOT NULL
);
INSERT INTO test182 VALUES (5141,71,'2003-11-18');
INSERT INTO test182 VALUES (5141,72,'2003-11-25');
INSERT INTO test182 VALUES (5141,52,'2004-08-06');
INSERT INTO test182 VALUES (5141,12,'2004-08-06');
INSERT INTO test182 VALUES (5141,87,'2004-10-21');
INSERT INTO test182 VALUES (5141,89,'2004-10-22');
INSERT INTO test182 VALUES (5141,41,'2004-08-06');
INSERT INTO test182 VALUES (5141,53,'2004-08-06');
INSERT INTO test182 VALUES (5141,86,'2004-10-21');
INSERT INTO test182 VALUES (5141,88,'2004-10-21');
INSERT INTO test182 VALUES (5141,51,'2004-10-26');

CREATE TABLE test183 (
  groupId int NOT NULL,
  parentId int NOT NULL,
  startDate date NOT NULL,
  endDate date NOT NULL,
  createDate date NOT NULL,
  modifyDate date NOT NULL,
  ordering int
);
INSERT INTO test183 VALUES (12,9,to_date('1000-01-01'),to_date('3999-12-31'),to_date('2004-01-29'),to_date('2004-01-29'),NULL);

CREATE TABLE test184 (
  id int NOT NULL,
  groupTypeId int NOT NULL,
  groupKey varchar2(50) NOT NULL,
  name varchar2(50),
  ordering int,
  description varchar2(50),
  createDate date NOT NULL,
  modifyDate date NOT NULL
);
INSERT INTO test184 VALUES (9,5,'stationer','stationer',0,'Stationer',to_date('2004-01-29'),to_date('2004-01-29'));
INSERT INTO test184 VALUES (12,5,'group2','group2',0,'group2',to_date('2004-01-29'),to_date('2004-01-29'));

CREATE TABLE test185 (
  userId int NOT NULL,
  groupId int NOT NULL,
  createDate date NOT NULL,
  modifyDate date NOT NULL
);
INSERT INTO test185 VALUES (5141,12,to_date('2004-08-06'),to_date('2004-08-06'));

--disable_warnings
--error 0,942
DROP TABLE test551 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test552 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test553 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test554 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test555 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test556 CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE test551(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO test551 VALUES(1,'AccounTing','New York');
INSERT INTO test551 VALUES (2,'ReSeaRch','DallAs');
INSERT INTO test551 VALUES(3,'SaLeS','ChiCaGo');
INSERT INTO test551 VALUES(4,'OperaTions','BoSton');

CREATE TABLE test552(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2) REFERENCES test551(DEPTNO),CHECK (sal > 500))
PARTITION BY RANGE(SAL) SUBPARTITION BY LIST(JOB)(PARTITION p1 VALUES less than(2500)(SUBPARTITION p1sub1 VALUES('CLERK'),SUBPARTITION p1sub2 VALUES('SALESMAN')),
PARTITION p2 VALUES less than(5000)(SUBPARTITION p2sub1 VALUES('MANAGER'),SUBPARTITION p2sub2 VALUES('ANALYST'),SUBPARTITION p2sub3 VALUES('PRESIDENT')	));
INSERT INTO test552 VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,100,2);
INSERT INTO test552 VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,3);
INSERT INTO test552 VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,3);
INSERT INTO test552 VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,358.69,2);
INSERT INTO test552 VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,3);
INSERT INTO test552 VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,3);
INSERT INTO test552 VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),4450,744.9,1);
INSERT INTO test552 VALUES(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,-200,2);
INSERT INTO test552 VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),4999,NULL,1);
INSERT INTO test552 VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,3);
INSERT INTO test552 VALUES(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,-99.9,2);
INSERT INTO test552 VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,-1808,3);
INSERT INTO test552 VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,753.9,2);
INSERT INTO test552 VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,729,1);

CREATE TABLE test553(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1))PARTITION BY RANGE(c1)SUBPARTITION BY HASH(c2)(PARTITION part10 VALUES LESS THAN(2)(SUBPARTITION p1sub3,SUBPARTITION p1sub4),
PARTITION part11 VALUES LESS THAN(5)(SUBPARTITION p1sub5,SUBPARTITION p1sub6));
INSERT INTO test553 VALUES  (1, 5, 'e', 8, NULL, 't', NULL, NULL) ;
INSERT INTO test553 VALUES  (2, 3, 'x', 2, 'q', 'k', 0, NULL) ;
INSERT INTO test553 VALUES  (2, 4, 'o', 5, 'w', 'u', 8,'n') ;
INSERT INTO test553 VALUES  (3, 4, 'h', 2, 'y', 'z', NULL, NULL) ;
INSERT INTO test553 VALUES  (4, 6, 'j', 2, 'h', 'p', 1,NULL) ;
INSERT INTO test553 VALUES  (4, 4, 'i', 2, 'd', 'x', 0,'f') ;
INSERT INTO test553 VALUES  (3, 9, 'e', 0, 'r', 'v', 6, 'd') ;
INSERT INTO test553 VALUES  (2, 9, 'y', 7, 'h', 'q', 2,'v') ;
INSERT INTO test553 VALUES  (1, 2, 'w', 1, 'c', 'l', 7,'o') ;
INSERT INTO test553 VALUES  (1, 9, 'y', 1, NULL, 's', 5, NULL) ;
INSERT INTO test553 VALUES  (2, 6, 'h', 7, NULL, 'f', 8, NULL) ;
INSERT INTO test553 VALUES  (4, 5, 'i', 2, NULL, 'w', NULL,'x') ;

CREATE TABLE test554(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO test554 SELECT * FROM test551 WHERE ROWNUM <= 3;

CREATE TABLE test555(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2),CHECK (sal > 500))PARTITION BY HASH(DEPTNO)(PARTITION part6,PARTITION part7,PARTITION part8);
INSERT INTO test555 SELECT * FROM(SELECT * FROM test552 ORDER BY empno) WHERE ROWNUM <= 6 ;

CREATE TABLE test556(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1));
CREATE UNIQUE INDEX index_34456 ON test556(c6);
CREATE INDEX index_4 ON test556(c1);
INSERT INTO test556 SELECT * FROM (SELECT * FROM test553 WHERE c3 IN ('w','e','y','x','i','h') ORDER BY 1,2,3,4);

--disable_warnings
--error 0,942
DROP TABLE Z0CASE;
--enable_warnings
CREATE  TABLE Z0CASE (z0_test0 varchar2(100),z0_test1 NUMBER ,z0_test2 binary_float)
PARTITION BY RANGE(z0_test2) (PARTITION p1 VALUES less than(3), PARTITION p2 VALUES less than(5), PARTITION p3 VALUES less than(MAXVALUE));


INSERT INTO Z0CASE VALUES ('ABCabc',0,1.1);
INSERT INTO Z0CASE VALUES ('123456',1,2.2);
INSERT INTO Z0CASE VALUES ('!@#$%^&*',2,3.333);
INSERT INTO Z0CASE VALUES ('中文字符123',3,4.4444);
INSERT INTO Z0CASE VALUES ('  ',4,5.5555);
INSERT INTO Z0CASE VALUES ('',5,66.666);

--disable_warnings
--error 0,942
DROP TABLE Z1CASE;
--enable_warnings
CREATE  TABLE Z1CASE (z1_test0 varchar2(100),z1_test1 NUMBER ,z1_test2 binary_float)
PARTITION BY RANGE(z1_test2) (PARTITION p1 VALUES less than(5), PARTITION p2 VALUES less than(10), PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO Z1CASE VALUES ('ABCabc',11,00);
INSERT INTO Z1CASE VALUES ('123456',10,0.11);
INSERT INTO Z1CASE VALUES ('!@#123$%^&*',9,1.11);
INSERT INTO Z1CASE VALUES ('中文字符123',8,2.22);
INSERT INTO Z1CASE VALUES ('  ',7,3.33);
INSERT INTO Z1CASE VALUES ('',6,4.44);
INSERT INTO Z1CASE VALUES ('abcabc',5,5.555);
INSERT INTO Z1CASE VALUES ('11111.1111',4,6.666);
INSERT INTO Z1CASE VALUES ('!@#$%^&*',3,7.777);
INSERT INTO Z1CASE VALUES ('abc中文 字符123',2,8.888);
INSERT INTO Z1CASE VALUES (' ',1,9.999);
INSERT INTO Z1CASE VALUES ('',0,111.111);

--disable_warnings
--error 0,00942
drop table grouping1;
--enable_warnings
CREATE TABLE grouping1 (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;
INSERT INTO grouping1 VALUES (1, 868548608, 0, to_date('2008-12-25 01:10:47','yyyy-mm-dd hh24:mi:ss'), '', 0);
INSERT INTO grouping1 VALUES (2, 1387003904, 538902528, to_date('2004-02-15 10:44:52','yyyy-mm-dd hh24:mi:ss'), 'rmmjkovqnoqh', -2114322432);
INSERT INTO grouping1 VALUES (3, -1678966784, 0, (Timestamp '2009-04-06 17:15:12.047025'), NULL, 1865613312);

--disable_warnings
--error 0,00942
drop table wf_test1;
--enable_warnings
CREATE TABLE wf_test1 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;
insert into wf_test1(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1611857920, -474021888, -566689792, NULL, 1, 'this', to_date('2005-02-05 21:11:43','yyyy-mm-dd hh24:mi:ss'), to_date('2009-07-27','yyyy-mm-dd'), 1225785344);
insert into wf_test1(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1588789248, 749535232, 351666176, '', 2, 's', to_date('2006-06-23 06:15:53','yyyy-mm-dd hh24:mi:ss'), to_date('2007-06-26','yyyy-mm-dd'), 0);

--disable_warnings
--error 0,00942
drop table grouping11;
--enable_warnings
CREATE TABLE grouping11(
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;

--disable_warnings
--error 0,00942
drop table cume_group;
--enable_warnings
create table cume_group (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
primary key (pk) ) partition by hash(pk) partitions 4;
insert into cume_group values (1, 868548608, 0, to_date('2008-12-25 01:10:47','yyyy-mm-dd hh24:mi:ss'), '', 0) ;
insert into cume_group values (2, 1387003904, 538902528, to_date('2004-02-15 10:44:52','yyyy-mm-dd hh24:mi:ss'), 'rmmjkovqnoqh', -2114322432) ;
insert into cume_group values (3, -1678966784, 0, (timestamp '2009-04-06 17:15:12.047025'), null, 1865613312) ;
insert into cume_group values (4, 1488257024, 782434304, to_date('2006-08-12 16:39:33','yyyy-mm-dd hh24:mi:ss'), 'i', -1414922240) ;
insert into cume_group values (5, 1508835328, 185335808, (timestamp '2002-07-12 19:18:28.007325'), 'x', 2096758784) ;
insert into cume_group values (6, -1587281920, 105971712, (timestamp '2001-12-19 06:21:34.036297'), 'so', 445972480) ;
insert into cume_group values (7, 167575552, -752877568, (timestamp '2006-11-22 15:47:47.010215'), 'm', 1246363648) ;
insert into cume_group values (8, -1590165504, -970457088, to_date('2008-04-16 03:59:05','yyyy-mm-dd hh24:mi:ss'), '', -429522944) ;
insert into cume_group values (9, -513409024, -803995648, (timestamp '2005-12-22 03:52:43.035922'), 'j', 0) ;
insert into cume_group values (10, -1531445248, 0, to_date('2001-08-15 21:22:47','yyyy-mm-dd hh24:mi:ss'), '', 0) ;
insert into cume_group values (11, -1583284224, 680329216, (timestamp '2009-12-23 19:40:09.048862'), 'z', 0) ;
insert into cume_group values (12, -582352896, -1330446336, (timestamp '2005-09-12 16:35:00.027314'), '', -391118848) ;
insert into cume_group values (13, 1471021056, 0, to_date('2008-04-11 02:33:14','yyyy-mm-dd hh24:mi:ss'), null, 398786560) ;
insert into cume_group values (14, -160432128, 622526464, to_date('2008-10-19','yyyy-mm-dd'), null, 303431680) ;
insert into cume_group values (15, 0, 942473216, null, '', 1044905984);
select pk,ceil(cumedistvalue) cumedistvalue,count(*) from (select pk,cume_dist() over (order by grouping(col_varchar2_20),grouping(pk) desc nulls last) cumedistvalue from cume_group where (COL_INT between 300 and 887266517765 and COL_DECIMAL_20_2 = 522654.38) or COL_DATE is NULL and abs(COL_NUMBER) <> 9999999 group by rollup(pk,col_varchar2_20) order by pk asc) group by pk,ceil(cumedistvalue);
select first_value (pk+col_int ignore nulls) over (partition by col_int order by col_int,pk desc nulls first) firstvalue from cume_group where (to_char(COL_DATE,'yyyymmdd') != 20191219 and COL_INT between 300 and 887266517765) or PK < 522654.38 and PK is NULL union select row_number() over (order by col_int,pk asc nulls last) rnumvalue from cume_group where (to_char(COL_DATE,'yyyymmdd') != 20191219 and COL_INT between 300 and 887266517765) or PK < 522654.38 and PK is NULL order by 1;

--disable_warnings
--error 0,00942
drop table grouping_rollup_t00;
--enable_warnings
create table grouping_rollup_t00 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) partition by hash(pk) partitions 4;
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1611857920, -474021888, -566689792, NULL, 1, 'this', to_date('2005-02-05 21:11:43','yyyy-mm-dd hh24:mi:ss'), to_date('2009-07-27','yyyy-mm-dd'), 1225785344);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1588789248, 749535232, 351666176, '', 2, 's', to_date('2006-06-23 06:15:53','yyyy-mm-dd hh24:mi:ss'), to_date('2007-06-26','yyyy-mm-dd'), 0);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1103495168, 384565248, 1958608896, '', 3, NULL, (Timestamp '2006-03-05 20:18:36.015514'), (Timestamp '2003-09-10 04:13:09.010524'), 844300288);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, 0, 0, 'q', 4, 'lhit', to_date('2001-11-10','yyyy-mm-dd'), (Timestamp '2000-02-10 21:45:18.021280'), -1576271872);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1646985216, 0, 0, 'm', 5, '', to_date('2004-09-25','yyyy-mm-dd'), (Timestamp '2007-05-16 19:15:59.034385'), 992542720);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values('', '', '', '', 6, '', '', '', '');


--disable_warnings
--error 0,00942
drop table grouping_rollup_t01;
--enable_warnings
create table grouping_rollup_t01 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) partition by hash(pk) partitions 10;
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, 0, 0, '', 8, '', NULL, (Timestamp '2004-03-12 21:25:48.044571'), 1668415488);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1455292416, -476250112, -1987248128, NULL, 9, 'she', to_date('2008-10-19','yyyy-mm-dd'), (Timestamp '2008-07-28 19:09:49.001817'), 798556160);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-2106064896, 148242432, -430309376, 'j', 10, 'rv', (Timestamp '2005-03-03 16:34:52.001480'), (Timestamp '2004-11-23 02:21:32.018522'), 0);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, -1016922112, 1504968704, 'w', 11, NULL, to_date('2007-04-21 16:13:18','yyyy-mm-dd hh24:mi:ss'), (Timestamp '2008-01-28 16:47:00.029273'), -718667776);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-759234560, 1424949248, -798031872, 'srvlhitckzgppy', 12, 'w', NULL, (Timestamp '2004-07-26 22:05:43.046113'), 0);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1652883456, 530972672, 1590362112, 'are', 13, NULL, to_date('2008-07-01','yyyy-mm-dd'), (Timestamp '2005-12-22 07:38:05.023325'), 0);

--disable_warnings
--error 0,00942
drop table first_last_value1;
--enable_warnings
create table first_last_value1(c0 number,c1 number,c2 varchar2(10),c3 varchar2(10),c4 date,c5 number generated always as (c1+c0)) partition by hash(c1) partitions 4;
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,1,'oraman','dba','2019-12-10');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,2,'ross','sa','2019-12-11');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,3,'james','sa','2019-12-11');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,4,'terry','sa','2019-12-13');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,5,'cattiny','sa','2019-12-14');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,8,'terry','cto','2019-12-12');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,10,'gc','dba','2019-12-10');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,11,'henry','dba','2019-12-10');

--disable_warnings
--error 0,00942
drop table lead_lag2;
--enable_warnings
create table lead_lag2(c1 varchar2(64) not null,c2 varchar2(10) not null,c3 date not null, primary key (c1));
insert into lead_lag2 values ('1000001', 'AB7477', to_date('2015-11-30 10:18:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000002', 'AB7477', to_date('2015-11-30 10:22:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000003', 'AB7477', to_date('2015-11-30 10:28:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000004', 'AB7477', to_date('2015-11-30 10:29:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000005', 'AB7477', to_date('2015-11-30 10:39:13','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000006', 'AB7477', to_date('2015-11-30 10:45:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000007', 'AB7477', to_date('2015-11-30 10:56:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000008', 'AB7477', to_date('2015-11-30 10:57:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000009', 'AB3808', to_date('2015-11-30 11:00:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000010', 'AB3808', to_date('2015-11-30 11:10:13','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000011', 'AB3808', to_date('2015-11-30 11:15:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000012', 'AB3808', to_date('2015-11-30 11:26:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000013', 'AB3808', to_date('2015-11-30 11:30:12','YYYY-MM-DD HH24:mi:ss'));

--disable_warnings
--error 0,00942
drop table wf_test111;
--enable_warnings
CREATE TABLE wf_test111 (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
/*Indices*/
primary key (pk));
insert INTO wf_test111 VALUES (1, 868548608, 0, (Timestamp '2008-12-25 01:10:47.050179'), 'there', 1387003904) ;
insert INTO wf_test111 VALUES (2, 538902528, 0, (Timestamp '2002-11-05 16:28:11.056788'), 'q', -475725824) ;
insert INTO wf_test111 VALUES (3, -1287520256, -361234432, (Timestamp '2000-07-20 05:30:47.051125'), 'back', -778567680) ;
insert INTO wf_test111 VALUES (4, -1103888384, -1961033728, (Timestamp '2004-03-07 14:02:48.037340'), 'w', -98959360) ;
insert INTO wf_test111 VALUES (5, 1601961984, -608960512, (Timestamp '2009-01-17 03:03:49.050687'), 'd', -868155392) ;
insert INTO wf_test111 VALUES (6, 1662976000, 0, (Timestamp '2001-11-28 00:50:27.051028'), 'the', -1465647104) ;
insert INTO wf_test111 VALUES (7, 0, 0, to_date('2002-11-01 20:02:37','yyyy-mm-dd hh24:mi:ss'), 'some', -298057728) ;
insert INTO wf_test111 VALUES (8, 1550123008, 2053308416, (Timestamp '2004-05-07 10:00:03.039599'), 'be', -1808990208) ;
COMMIT;




connection conn_dblink_tnt2;
--echo #dblink	普通表

--disable_warnings
--error 0,942
drop table EMP;
--error 0,942
drop table DEPT;
--enable_warnings

CREATE TABLE DEPT(DEPTNO NUMBER(2),DNAME VARCHAR2(14),LOC VARCHAR2(13));
CREATE TABLE EMP(EMPNO NUMBER(4) ,ENAME VARCHAR2(10),JOB VARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);

--disable_warnings
--error 0,942
drop table olympic_medal_winners;
--enable_warnings

create table olympic_medal_winners(   
  olympic_year int,  
  sport        varchar2(30),  
  gender       varchar2(1),  
  event        varchar2(128),  
  medal        varchar2(10),  
  noc          varchar2(3),  
  athlete      varchar2(128) 
);

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Gold','KOR','KU Bonchan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Silver','FRA','VALLADONT Jean-Charles');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Individual','Bronze','USA','ELLISON Brady');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Gold','KOR','Republic of Korea');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Bronze','AUS','Australia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Archery','M','Men''s Team','Silver','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Gold','GBR','WHITLOCK Max');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Bronze','BRA','MARIANO Arthur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Floor Exercise','Silver','BRA','HYPOLITO Diego');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Gold','GER','HAMBUECHEN Fabian');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Bronze','GBR','WILSON Nile');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Artistic Gymnastics','M','Men''s Horizontal Bar','Silver','USA','LEYVA Danell');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Gold','GBR','FARAH Mohamed');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Bronze','ETH','TOLA Tamirat');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 10,000m','Silver','KEN','TANUI Paul Kipngetich');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Gold','JAM','BOLT Usain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Silver','USA','GATLIN Justin');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Athletics','M','Men''s 100m','Bronze','CAN','DE GRASSE Andre');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Gold','CHN','Zhang');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Bronze','GBR','Langridge');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Bronze','GBR','Ellis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Silver','MAS','Tan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Silver','MAS','Goh');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Badminton','M','Men''s Doubles','Gold','CHN','Fu');
select count(*) from olympic_medal_winners;
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Gold','BRA','Cerutti');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Gold','BRA','Oscar Schmidt');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Silver','ITA','Nicolai');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Silver','ITA','Lupo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Bronze','NED','Meeuwsen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Beach Volleyball','M','Men','Bronze','NED','Brouwer');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Gold','CUB','RAMIREZ Robeisy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Bronze','UZB','AKHMADALIEV Murodjon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Bronze','RUS','NIKITIN Vladimir');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Bantam (56kg)','Silver','USA','STEVENSON Shakur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Fly (52kg)','Gold','UZB','ZOIROV Shakhobidin');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Boxing','M','Men''s Fly (52kg)','Bronze','CHN','HU Jianguan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Gold','SVK','PETER Skantar');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Bronze','FRA','GAUTHIER Klauss');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Bronze','FRA','MATTHIEU Peche');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Silver','GBR','RICHARD Hounslow');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Silver','GBR','DAVID Florence');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Slalom','M','Canoe Double (C2) Men','Gold','SVK','LADISLAV Skantar');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Gold','GER','Brendel');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Bronze','UKR','Mishchuk');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Bronze','UKR','Ianchuk');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Silver','BRA','Queiroz dos Santos');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Silver','BRA','de Souza Silva');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Canoe Sprint','M','Men''s Canoe Double 1000m','Gold','GER','Vandrey');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Gold','SUI','CANCELLARA Fabian');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Bronze','GBR','FROOME Christopher');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Individual Time Trial','Silver','NED','DUMOULIN Tom');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Gold','BEL','VAN AVERMAET Greg');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Silver','DEN','FUGLSANG Jakob');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Road','M','Men''s Road Race','Bronze','POL','MAJKA Rafal');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Gold','GBR','KENNY Jason');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Bronze','MAS','AWANG Azizulhasni');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Keirin','Silver','NED','BUCHLI Matthijs');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Gold','ITA','VIVIANI Elia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Bronze','DEN','HANSEN Lasse Norman');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Cycling Track','M','Men''s Omnium','Silver','GBR','CAVENDISH Mark');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Gold','CHN','CHEN Aisen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Bronze','USA','BOUDIA David');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 10m Platform','Silver','MEX','SANCHEZ German');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Gold','CHN','CAO Yuan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Silver','GBR','LAUGHER Jack');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Diving','M','Men''s 3m Springboard','Bronze','GER','HAUSDING Patrick');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Gold','GBR','DUJARDIN Charlotte');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Bronze','GER','BRORING-SPREHE Kristina');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Individual','Silver','GER','WERTH Isabell');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Gold','GER','Germany');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Equestrian','X','Dressage Team','Silver','GBR','Great Britain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Gold','ITA','GAROZZO Daniele');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Silver','USA','MASSIALAS Alexander');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Individual','Bronze','RUS','SAFIN Timur');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Fencing','M','Men''s Foil Team','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Gold','DEN','Denmark');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','M','Men','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Silver','FRA','France');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Handball','W','Women','Bronze','NOR','Norway');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Gold','ARG','Argentina');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Silver','BEL','Belgium');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','M','Men','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Gold','GBR','Great Britain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Silver','NED','Netherlands');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Hockey','W','Women','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Gold','FRA','RINER Teddy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Bronze','BRA','SILVA Rafael');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Bronze','ISR','SASSON Or');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men +100 kg','Silver','JPN','HARASAWA Hisayoshi');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men -100 kg','Gold','CZE','KRPALEK Lukas');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Judo','M','Men -100 kg','Bronze','FRA','MARET Cyrille');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Gold','RUS','LESUN Alexander');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Silver','UKR','TYMOSHCHENKO Pavlo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','M','Men''s Individual','Bronze','MEX','HERNANDEZ USCANGA Ismael Marcelo');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Gold','AUS','ESPOSITO Chloe');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Silver','FRA','CLOUVEL Elodie');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Modern Pentathlon','W','Women''s Individual','Bronze','POL','NOWACKA Oktawia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Gold','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Bronze','BUL','Bulgaria');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Group All-Around','Silver','ESP','Spain');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Gold','RUS','MAMUN Margarita');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Silver','RUS','KUDRYAVTSEVA Yana');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rhythmic Gymnastics','W','Individual All-Around','Bronze','UKR','RIZATDINOVA Ganna');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Gold','FRA','Azou');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Bronze','NOR','Brun');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Bronze','NOR','Strandli');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Silver','IRL','O''Donovan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Silver','IRL','O''Donovan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Rowing','M','Lightweight Men''s Double Sculls','Gold','FRA','Houin');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Gold','CRO','Fantela');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Bronze','GRE','Kagialis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Bronze','GRE','Mantis');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Silver','AUS','Ryan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Silver','AUS','Belcher');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Sailing','M','470 Men','Gold','CRO','Marenic');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Gold','USA','MURPHY Ryan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Bronze','USA','PLUMMER David');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Backstroke','Silver','CHN','XU Jiayu');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Gold','GBR','PEATY Adam');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Bronze','USA','MILLER Cody');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Swimming','M','Men''s 100m Breaststroke','Silver','RSA','VAN DER BURGH Cameron');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Gold','CHN','MA Long');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Bronze','JPN','MIZUTANI Jun');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Singles','Silver','CHN','ZHANG Jike');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Gold','CHN','China');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Bronze','GER','Germany');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Table Tennis','M','Men''s Team','Silver','JPN','Japan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Gold','AZE','ISAEV Radik');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Bronze','KOR','CHA Dongmin') ;
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Bronze','BRA','SIQUEIRA Maicon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men +80kg','Silver','NIG','ISSOUFOU ALFAGA Abdoulrazak');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men -58kg','Gold','CHN','ZHAO Shuai');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Taekwondo','M','Men -58kg','Silver','THA','HANPRAB Tawin');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Gold','ESP','Lopez');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Bronze','USA','Johnson');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Bronze','USA','Sock');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Silver','ROU','Tecau');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Silver','ROU','Mergea');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Tennis','M','Men''s Doubles','Gold','ESP','Nadal');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Gold','BLR','HANCHAROU Uladzislau');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Silver','CHN','DONG Dong');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','M','Men','Bronze','CHN','GAO Lei');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Gold','CAN','MACLENNAN Rosannagh');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Silver','GBR','PAGE Bryony');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Trampoline Gymnastics','W','Women','Bronze','CHN','LI Dan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Gold','GBR','BROWNLEE Alistair');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Silver','GBR','BROWNLEE Jonathan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','M','Men','Bronze','RSA','SCHOEMAN Henri');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Gold','USA','JORGENSEN Gwen');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Silver','SUI','SPIRIG HUG Nicola');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Triathlon','W','Women','Bronze','GBR','HOLLAND Vicky');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Gold','BRA','Brazil');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Silver','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','M','Men','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Gold','CHN','China');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Silver','SRB','Serbia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Volleyball','W','Women','Bronze','USA','United States');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Gold','SRB','Serbia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Silver','CRO','Croatia');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','M','Men','Bronze','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Gold','USA','United States');

Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Silver','ITA','Italy');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Water Polo','W','Women','Bronze','RUS','Russian Federation');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Gold','GEO','TALAKHADZE Lasha');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Bronze','GEO','TURMANIDZE Irakli');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s +105kg','Silver','ARM','MINASYAN Gor');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Gold','UZB','NURUDINOV Ruslan');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Bronze','KAZ','ZAICHIKOV Alexandr');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Weightlifting','M','Men''s 105kg','Silver','ARM','MARTIROSYAN Simon');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Gold','TUR','AKGUL Taha');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Bronze','BLR','SAIDAU Ibrahim');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Bronze','GEO','PETRIASHVILI Geno');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 125 kg','Silver','IRI','GHASEMI Komeil Nemat');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 57 kg','Gold','GEO','KHINCHEGASHVILI Vladimer');
Insert into olympic_medal_winners (OLYMPIC_YEAR,SPORT,GENDER,EVENT,MEDAL,NOC,ATHLETE) values (2016,'Wrestling','M','Men''s Freestyle 57 kg','Bronze','AZE','ALIYEV Haji');

--error 0,942
DROP TABLE test200 CASCADE CONSTRAINTS purge;
CREATE TABLE test200(f1 int, f2 varchar2(10), f3 varchar(10));
INSERT INTO test200 VALUES(1001,'d','12345');
INSERT INTO test200 VALUES(1002,'e','67890');
INSERT INTO test200 VALUES(1006,'f','23456');
commit;

--error 0,942
DROP TABLE test201 CASCADE CONSTRAINTS purge;
--echo #list分区
CREATE TABLE test201(f1 int, f2 nvarchar2(10), f3 varchar(10)) 
PARTITION BY list(f1) (PARTITION p1 VALUES (1001,1006),PARTITION p2 VALUES (1002,1005,1008),PARTITION p3 VALUES (DEFAULT));
INSERT INTO test201 VALUES(1001,'d','12345');
INSERT INTO test201 VALUES(1002,'e','67890');
INSERT INTO test201 VALUES(1006,'f','23456');
INSERT INTO test201 VALUES(1007,'g','23456');
--echo #同构二级分区（range-hash）
--error 0,942
DROP TABLE test202 CASCADE CONSTRAINTS purge;
CREATE TABLE test202(f1 NUMBER(5,0), f2 nvarchar2(10), f3 varchar(10)) 
PARTITION BY RANGE(f1) subpartition BY hash(f2) subpartitions 2 (PARTITION p1 VALUES less than (1004),PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test202 VALUES(1001,'a','12345');
INSERT INTO test202 VALUES(1003,'b','67890');
INSERT INTO test202 VALUES(1004,'c','23456');
INSERT INTO test202 VALUES(1006,'d','23456');
INSERT INTO test202 VALUES(1007,'e','23456');

--error 0,942
DROP TABLE test203 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test204 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test205 CASCADE CONSTRAINTS purge;

CREATE TABLE test203(f1 int, f2 nvarchar2(10), f3 varchar(10));
INSERT INTO test203 VALUES(1001,'a','12345');
INSERT INTO test203 VALUES(1003,'b','67890');
INSERT INTO test203 VALUES(1004,'c','23456');
INSERT INTO test203 VALUES(1006,'d','23456');
INSERT INTO test203 VALUES(1007,'e','23456');
--echo #for oltp压缩
CREATE TABLE test204 COMPRESS FOR oltp AS SELECT * FROM test203;
--echo #for archive压缩
###CREATE TABLE test205 COMPRESS FOR archive AS SELECT * FROM test203;
commit;

--error 0,942
DROP TABLE t206 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test207 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test208 CASCADE CONSTRAINTS purge;
CREATE TABLE t206(f1 NUMBER PRIMARY KEY);
INSERT INTO t206 VALUES(1e10);
INSERT INTO t206 VALUES(1e-10);
INSERT INTO t206 VALUES(5);
CREATE TABLE test207(f1 NUMBER(15));
INSERT INTO test207 VALUES(1e10);
INSERT INTO test207 VALUES(1e-10);
INSERT INTO test207 VALUES(5);
CREATE TABLE test208(f1 NUMBER(15,12));
INSERT INTO test208 VALUES(100);
INSERT INTO test208 VALUES(1e-10);
INSERT INTO test208 VALUES(5.5);

--error 0,942
DROP TABLE test209 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test210 CASCADE CONSTRAINTS purge;
CREATE TABLE test209(f1 float PRIMARY KEY) PARTITION BY RANGE(f1) (PARTITION p1 VALUES less than (5), PARTITION p2 VALUES less than (10), PARTITION p3 VALUES less than (MAXVALUE));
INSERT INTO test209 VALUES(1e10);
INSERT INTO test209 VALUES(1e-10);
INSERT INTO test209 VALUES(6);
CREATE TABLE test210(f1 float(15))PARTITION BY hash(f1) PARTITIONS 2;
INSERT INTO test210 VALUES(1e10);
INSERT INTO test210 VALUES(1e-10);
INSERT INTO test210 VALUES(6);

--error 0,942
DROP TABLE test211 CASCADE CONSTRAINTS purge;

CREATE TABLE test211(f1 binary_float PRIMARY KEY) PARTITION BY list(f1) (PARTITION p1 VALUES(1e10,5), PARTITION p2 VALUES(DEFAULT));
INSERT INTO test211 VALUES(1e10);
INSERT INTO test211 VALUES(1e-10);
INSERT INTO test211 VALUES(5);

--error 0,942
DROP TABLE test212 CASCADE CONSTRAINTS purge;
CREATE TABLE test212(f1 binary_double) PARTITION BY RANGE(f1) (PARTITION p1 VALUES less than (10), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test212 VALUES(1e10);
INSERT INTO test212 VALUES(1e-10);
INSERT INTO test212 VALUES(5);

--error 0,942
DROP TABLE test213 CASCADE CONSTRAINTS purge;
CREATE TABLE test213(f1 varchar2(5 byte)) COMPRESS FOR oltp;
INSERT INTO test213 VALUES('111');
INSERT INTO test213 VALUES('aaa');
INSERT INTO test213 VALUES('666');

connection conn_dblink_tnt2;
--error 0,942
DROP TABLE test214 CASCADE CONSTRAINTS purge;
CREATE TABLE test214(f1 nvarchar2(5)) COMPRESS FOR oltp;
INSERT INTO test214 VALUES('11');
INSERT INTO test214 VALUES('aa');
INSERT INTO test214 VALUES('66');

--error 0,942
DROP TABLE test215 CASCADE CONSTRAINTS purge;
CREATE TABLE test215(f1 nvarchar2(5)) COMPRESS FOR oltp;
INSERT INTO test215 VALUES('11');
INSERT INTO test215 VALUES('aa');
INSERT INTO test215 VALUES('66');

--error 0,942
DROP TABLE test216 CASCADE CONSTRAINTS purge;
CREATE TABLE test216(f1 int,f2 DATE,CONSTRAINT pk_233 PRIMARY KEY(f2)) PARTITION BY RANGE(f2) (PARTITION p1 VALUES less than (to_date('25-07-2008 12:01:00','dd-mm-yyyy hh24:mi:ss')), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test216 VALUES(101,to_date('25-09-2005 12:01:00','dd-mm-yyyy hh24:mi:ss'));
INSERT INTO test216 VALUES(102,to_date('25-09-2008 12:01:00','dd-mm-yyyy hh24:mi:ss'));
INSERT INTO test216 VALUES(103,to_date('25-09-2018 12:01:00','dd-mm-yyyy hh24:mi:ss'));

--error 0,942
DROP TABLE test217 CASCADE CONSTRAINTS purge;
CREATE TABLE test217(f1 int,f2 timestamp(5),CONSTRAINT pk777 PRIMARY KEY(f1,f2));
INSERT INTO test217 VALUES(101,to_timestamp('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test217 VALUES(103,to_timestamp('25-09-2009 12:01:44.44444444','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test217 VALUES(105,to_timestamp('25-09-2020 12:01:55.55555555','dd-mm-yyyy hh24:mi:ss.ff'));

--error 0,942
DROP TABLE test218 CASCADE CONSTRAINTS purge;
CREATE TABLE test218(f1 int,f2 INTERVAL YEAR(5) TO MONTH,CONSTRAINT pk788 PRIMARY KEY(f1,f2));
INSERT INTO test218 VALUES(101,numtoyminterval(1,'year')+numtoyminterval(1,'month'));
INSERT INTO test218 VALUES(103,numtoyminterval(2,'year')+numtoyminterval(1,'month'));
INSERT INTO test218 VALUES(105,numtoyminterval(3,'year')+numtoyminterval(1,'month'));

###--error 0,942
###DROP TABLE test219 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test219(f1 int,f2 LONG);
###INSERT INTO test219 VALUES(123,'adminadminadmin');
###INSERT INTO test219 VALUES(456,'testtesttest');

--error 0,942
DROP TABLE test220 CASCADE CONSTRAINTS purge;
CREATE TABLE test220(f1 int,f2 raw(20) PRIMARY KEY) COMPRESS FOR oltp;
INSERT INTO test220 VALUES(101,utl_raw.cast_to_raw('rawtype'));
INSERT INTO test220 VALUES(103,utl_raw.cast_to_raw('1234567'));

###--error 0,942
###DROP TABLE test221 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test221(f1 int,f2 long raw);
###INSERT INTO test221 VALUES(101,utl_raw.cast_to_raw('rawtype'));
###INSERT INTO test221 VALUES(103,utl_raw.cast_to_raw('1234567'));
###INSERT INTO test221 VALUES(104,utl_raw.cast_to_raw('4841565'));

###--error 0,942
###DROP TABLE test222 CASCADE CONSTRAINTS purge;
###CREATE TABLE test222(f1 int,f2 rowid);
###INSERT INTO test222 VALUES(1,'AAKAdoAAEAADFlAAAA');
###INSERT INTO test222 VALUES(2,'AAKAdoAAEAADFlAAAB');

--error 0,942
DROP TABLE test223 CASCADE CONSTRAINTS purge;
CREATE TABLE test223(f1 int,f2 clob);
INSERT INTO test223 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test223 VALUES(2,'fal;jg;agopra/;.''po[ragp');
INSERT INTO test223 VALUES(5,'1234ddddddddddddddddddddddddddd');
INSERT INTO test223 VALUES(7,'fal;jg;agopra/;.''po[ragp');

--error 0,942
DROP TABLE test224 CASCADE CONSTRAINTS purge;
CREATE TABLE test224(f1 int PRIMARY KEY,f2 blob);
INSERT INTO test224 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test224 VALUES(2,'41895fdabccccccccffff87897686aa');
INSERT INTO test224 VALUES(5,'1234ddddddddddddddddddddddddddd');
INSERT INTO test224 VALUES(7,'41895fdabccccccccffff87897686aa');

###--error 0,942
###DROP TABLE test224 CASCADE CONSTRAINTS purge;
###--error 900
###CREATE TABLE test224(f1 int,f2 nclob);
###INSERT INTO test224 VALUES(1,'AAJDTjAAEAAITskAAA');
###INSERT INTO test224 VALUES(2,'AAJDTnAAEAAITtcAAB');

--error 0,942
DROP TABLE test225 CASCADE CONSTRAINTS purge;
CREATE TABLE test225(f1 NUMBER(10), f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(5));
INSERT INTO test225 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test225 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test225 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test225 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

--error 0,942
DROP TABLE test226 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test227 CASCADE CONSTRAINTS purge;

CREATE TABLE test226(f1 int PRIMARY KEY);
INSERT INTO test226 VALUES(1001);
INSERT INTO test226 VALUES(1002);
INSERT INTO test226 VALUES(1003);
CREATE TABLE test227(
	f1 NUMBER(10) PRIMARY KEY, 
	f2 binary_float UNIQUE, 
	f3 varchar2(20) NULL, 
	f4 nvarchar2(20) NOT NULL, 
	f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
	f6 int,
	CONSTRAINT fk_2555 FOREIGN KEY(f6) REFERENCES test226(f1),
	CONSTRAINT check25555 CHECK (f2 BETWEEN power(10,2) AND power(10,3)))
PARTITION BY RANGE(f1) 
	(PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (100000));
INSERT INTO test227 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test227 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test227 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test227 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);

--error 0,942
DROP TABLE test228 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test229 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test230 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test231 CASCADE CONSTRAINTS purge;

CREATE TABLE test228(f1 int PRIMARY KEY);
INSERT INTO test228 VALUES(1001);
INSERT INTO test228 VALUES(1002);
INSERT INTO test228 VALUES(1003);

CREATE TABLE test229(f1 NUMBER(10), f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9));
CREATE INDEX index_4 ON test229(f2) GLOBAL PARTITION BY RANGE(f2) (PARTITION p1 VALUES less than (400), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test229 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test229 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test229 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test229 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

CREATE TABLE test230(f1 NUMBER(10) PRIMARY KEY, f2 binary_float, f3 varchar2(20), f4 nvarchar2(20), f5 timestamp(9)) PARTITION BY hash(f1) PARTITIONS 2;
CREATE INDEX index_5 ON test230(f2) GLOBAL PARTITION BY RANGE(f2) (PARTITION p1 VALUES less than (400), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test230 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test230 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test230 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test230 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

CREATE TABLE test231(
	f1 NUMBER(10), 
	f2 binary_float, 
	f3 varchar2(20) UNIQUE, 
	f4 nvarchar2(20) NOT NULL, 
	f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
	f6 int,
	CONSTRAINT fk_3888 FOREIGN KEY(f6) REFERENCES test228(f1),
	CONSTRAINT check2888 CHECK (f2 BETWEEN power(10,2) AND power(10,3)))
PARTITION BY RANGE(f1) 
	(PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (maxvalue));
CREATE INDEX index_6 ON test231(f2) LOCAL;
CREATE INDEX index_7 ON test231(f6) GLOBAL PARTITION BY range(f6) (PARTITION p1 VALUES less than (1002),PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test231 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test231 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test231 VALUES(12345.67890, 483.35433, '456782','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test231 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);


--error 0,942
DROP TABLE test232 CASCADE CONSTRAINTS purge;

CREATE TABLE test232(
	f1 NUMBER(10,5) PRIMARY KEY, 
	f2 binary_float, 
	f3 varchar2(20) UNIQUE, 
	f4 nvarchar2(20) NOT NULL, 
	f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
	f6 int,
	CONSTRAINT check22556 CHECK (f2 BETWEEN power(10,2) AND power(10,3)))
PARTITION BY RANGE(f1) 
	(PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (MAXVALUE));
CREATE INDEX index_234 ON test232(f2) LOCAL;
CREATE INDEX index_345 ON test232(f6) GLOBAL PARTITION BY range(f6) (PARTITION p1 VALUES less than (1002),PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test232 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test232 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test232 VALUES(12345.67890, 483.35433, '456782','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test232 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);

--error 0,942
DROP TABLE class233 CASCADE CONSTRAINTS purge;

CREATE TABLE class233 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY hash(id) PARTITIONS 2;
CREATE INDEX index_2133 ON class233(create_time) GLOBAL PARTITION BY RANGE(create_time) (PARTITION p1 VALUES less than (to_timestamp('25-09-2015 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff')), PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO class233 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

--error 0,942
DROP TABLE stu233 CASCADE CONSTRAINTS purge;
CREATE TABLE stu233 (id int, p_id int,name nvarchar2(50) NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int,
	CONSTRAINT fk_233 FOREIGN KEY(c_id) REFERENCES class233(id),
	CONSTRAINT check233 CHECK (weight BETWEEN 10 AND 100));
CREATE INDEX index_2233 ON stu233(birth) GLOBAL;
INSERT INTO stu233 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu233 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu233 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu233 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu233 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu233 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu233 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--enable_warnings

create table gbktable
(
c0 int,
c1 varchar2(200),
c2 nchar(200),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 VARCHAR2(200),
c8 raw(200),
c9 number(10,3),
CONSTRAINT PK_test2111 PRIMARY KEY (c1,c2)
);

insert into gbktable values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.2);
insert into gbktable values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee',100);
insert into gbktable values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff',100.3);
insert into gbktable values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc',200);
insert into gbktable values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.22);
insert into gbktable values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',3000);
insert into gbktable values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',2000.33);
insert into gbktable values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',1000);
insert into gbktable values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200',10000);
insert into gbktable values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300',999999);
insert into gbktable values(11,'中文测试011','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);

--echo #dblink
--disable_warnings
--error 0,942
DROP TABLE test241 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test242 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test243 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test244 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test241 (a int);
create table test242 (a int, b int);
create table test243 (a int);
create table test244 (a int not null, b int not null);
insert into test241 values (2);
insert into test242 values (1,7);
insert into test242 values (2,7);
insert into test243 values (6);
insert into test243 values (7);
insert into test243 values (3);
insert into test244 values (4,8);
insert into test244 values (3,8);
insert into test244 values (5,9);

--echo #dblink  
--disable_warnings
--error 0,942
DROP TABLE test161 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test162 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test163 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test164 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test165 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test166 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test167 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test168 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test169 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test170 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test171 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test172 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test173 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test174 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test175 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test176 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test177 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test161 (a int, b int);
create table test162 (a int, b int);
create table test163 (a int, b int);
insert into test161 values (0,100);
insert into test161 values (1,2);
insert into test161 values (1,3);
insert into test161 values (2,2);
insert into test161 values (2,7);
insert into test161 values (2,-1);
insert into test161 values (3,10);
insert into test162 values (0,0);
insert into test162 values (1,1);
insert into test162 values (2,1);
insert into test162 values (3,1);
insert into test162 values (4,1);
insert into test163 values (3,3);
insert into test163 values (2,2);
insert into test163 values (1,1);

create table test164 (s1 int);
create table test165 (s1 int);
insert into test164 values (1);
insert into test165 values (1);

CREATE TABLE test166 (id int default NULL,name varchar2(20) default NULL);
INSERT INTO test166 VALUES (1,'Tim');
INSERT INTO test166 VALUES (2,'Rebecca');
INSERT INTO test166 VALUES (3,NULL);
CREATE TABLE test167 (id int default NULL, pet varchar2(20) default NULL);
INSERT INTO test167 VALUES (1,'Fido');
INSERT INTO test167 VALUES (2,'Spot');
INSERT INTO test167 VALUES (3,'Felix');

create table test168 (i int, j number(10));
insert into test168 values (1, 2);
insert into test168 values (2, 2);
insert into test168 values (3, 2);

CREATE TABLE test169 (a int, b INT, d INT, c VARCHAR2(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO test169 VALUES (1,1,0,'a');
INSERT INTO test169 VALUES (1,2,0,'b');
INSERT INTO test169 VALUES (1,3,0,'c');
INSERT INTO test169 VALUES (1,4,0,'d');
INSERT INTO test169 VALUES (1,5,0,'e');
INSERT INTO test169 VALUES (2,1,0,'f');
INSERT INTO test169 VALUES (2,2,0,'g');
INSERT INTO test169 VALUES (2,3,0,'h');
INSERT INTO test169 VALUES (3,4,0,'i');
INSERT INTO test169 VALUES (3,3,0,'j');
INSERT INTO test169 VALUES (3,2,0,'k');
INSERT INTO test169 VALUES (3,1,0,'l');
INSERT INTO test169 VALUES (1,9,0,'m');
INSERT INTO test169 VALUES (1,0,10,'n');
INSERT INTO test169 VALUES (2,0,5,'o');
INSERT INTO test169 VALUES (3,0,7,'p');

CREATE TABLE test170 (id int NOT NULL, st VARCHAR2(2));
INSERT INTO test170 VALUES (3,'FL');
INSERT INTO test170 VALUES (2,'GA');
INSERT INTO test170 VALUES (4,'FL');
INSERT INTO test170 VALUES (1,'GA');
INSERT INTO test170 VALUES (5,'NY');
INSERT INTO test170 VALUES (7,'FL');
INSERT INTO test170 VALUES (6,'NY');

CREATE TABLE test171 (id int NOT NULL);
INSERT INTO test171 VALUES (7);
INSERT INTO test171 VALUES (5);
INSERT INTO test171 VALUES (1);
INSERT INTO test171 VALUES (3);

create table test172 (a int, b int);
insert into test172 values (0,0);
insert into test172 values (2,2);
insert into test172 values (3,3);

create table test173 (a int, b int);
insert into test173 values (1,1);
insert into test173 values (3,3);
insert into test173 values (NULL,4);

CREATE TABLE test174 (
  pk INT,
  col_varchar VARCHAR2(1),
  PRIMARY KEY (pk)
)
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(1,'w')
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(2,'m')
;

CREATE TABLE test175 (c1 VARCHAR2(1), c2 INT, c3 INT);
INSERT/*insert trace*/ INTO test175 VALUES ('',6,8);
INSERT/*insert trace*/ INTO test175 VALUES ('',75,NULL);

CREATE TABLE test176 (c1 VARCHAR2(1));
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');

CREATE TABLE test177 (i1 INT);
INSERT/*insert trace*/ INTO test177 VALUES (2);
INSERT/*insert trace*/ INTO test177 VALUES (4);
INSERT/*insert trace*/ INTO test177 VALUES (6);
INSERT/*insert trace*/ INTO test177 VALUES (8);


--disable_warnings
--error 0,942
DROP TABLE test551 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test552 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test553 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test554 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test555 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test556 CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE test551(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO test551 VALUES(1,'AccounTing','New York');
INSERT INTO test551 VALUES (2,'ReSeaRch','DallAs');
INSERT INTO test551 VALUES(3,'SaLeS','ChiCaGo');
INSERT INTO test551 VALUES(4,'OperaTions','BoSton');

CREATE TABLE test552(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2) REFERENCES test551(DEPTNO),CHECK (sal > 500))
PARTITION BY RANGE(SAL) SUBPARTITION BY LIST(JOB)(PARTITION p1 VALUES less than(2500)(SUBPARTITION p1sub1 VALUES('CLERK'),SUBPARTITION p1sub2 VALUES('SALESMAN')),
PARTITION p2 VALUES less than(5000)(SUBPARTITION p2sub1 VALUES('MANAGER'),SUBPARTITION p2sub2 VALUES('ANALYST'),SUBPARTITION p2sub3 VALUES('PRESIDENT')	));
INSERT INTO test552 VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,100,2);
INSERT INTO test552 VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,3);
INSERT INTO test552 VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,3);
INSERT INTO test552 VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,358.69,2);
INSERT INTO test552 VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,3);
INSERT INTO test552 VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,3);
INSERT INTO test552 VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),4450,744.9,1);
INSERT INTO test552 VALUES(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,-200,2);
INSERT INTO test552 VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),4999,NULL,1);
INSERT INTO test552 VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,3);
INSERT INTO test552 VALUES(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,-99.9,2);
INSERT INTO test552 VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,-1808,3);
INSERT INTO test552 VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,753.9,2);
INSERT INTO test552 VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,729,1);

CREATE TABLE test553(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1))PARTITION BY RANGE(c1)SUBPARTITION BY HASH(c2)(PARTITION part10 VALUES LESS THAN(2)(SUBPARTITION p1sub3,SUBPARTITION p1sub4),
PARTITION part11 VALUES LESS THAN(5)(SUBPARTITION p1sub5,SUBPARTITION p1sub6));
INSERT INTO test553 VALUES  (1, 5, 'e', 8, NULL, 't', NULL, NULL) ;
INSERT INTO test553 VALUES  (2, 3, 'x', 2, 'q', 'k', 0, NULL) ;
INSERT INTO test553 VALUES  (2, 4, 'o', 5, 'w', 'u', 8,'n') ;
INSERT INTO test553 VALUES  (3, 4, 'h', 2, 'y', 'z', NULL, NULL) ;
INSERT INTO test553 VALUES  (4, 6, 'j', 2, 'h', 'p', 1,NULL) ;
INSERT INTO test553 VALUES  (4, 4, 'i', 2, 'd', 'x', 0,'f') ;
INSERT INTO test553 VALUES  (3, 9, 'e', 0, 'r', 'v', 6, 'd') ;
INSERT INTO test553 VALUES  (2, 9, 'y', 7, 'h', 'q', 2,'v') ;
INSERT INTO test553 VALUES  (1, 2, 'w', 1, 'c', 'l', 7,'o') ;
INSERT INTO test553 VALUES  (1, 9, 'y', 1, NULL, 's', 5, NULL) ;
INSERT INTO test553 VALUES  (2, 6, 'h', 7, NULL, 'f', 8, NULL) ;
INSERT INTO test553 VALUES  (4, 5, 'i', 2, NULL, 'w', NULL,'x') ;

CREATE TABLE test554(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO test554 SELECT * FROM test551 WHERE ROWNUM <= 3;

CREATE TABLE test555(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2),CHECK (sal > 500))PARTITION BY HASH(DEPTNO)(PARTITION part6,PARTITION part7,PARTITION part8);
INSERT INTO test555 SELECT * FROM(SELECT * FROM test552 ORDER BY empno) WHERE ROWNUM <= 6 ;

CREATE TABLE test556(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1));
CREATE UNIQUE INDEX index_335 ON test556(c6);
CREATE INDEX index_433 ON test556(c1);
INSERT INTO test556 SELECT * FROM (SELECT * FROM test553 WHERE c3 IN ('w','e','y','x','i','h') ORDER BY 1,2,3,4);

--disable_warnings
--error 0,942
DROP TABLE Z0CASE;
--enable_warnings
CREATE  TABLE Z0CASE (z0_test0 varchar2(100),z0_test1 NUMBER ,z0_test2 binary_float)
PARTITION BY RANGE(z0_test2) (PARTITION p1 VALUES less than(3), PARTITION p2 VALUES less than(5), PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO Z0CASE VALUES ('ABCabc',0,1.1);
INSERT INTO Z0CASE VALUES ('123456',1,2.2);
INSERT INTO Z0CASE VALUES ('!@#$%^&*',2,3.333);
INSERT INTO Z0CASE VALUES ('中文字符123',3,4.4444);
INSERT INTO Z0CASE VALUES ('  ',4,5.5555);
INSERT INTO Z0CASE VALUES ('',5,66.666);

--disable_warnings
--error 0,942
DROP TABLE Z1CASE;
--enable_warnings
CREATE  TABLE Z1CASE (z1_test0 varchar2(100),z1_test1 NUMBER ,z1_test2 binary_float)
PARTITION BY RANGE(z1_test2) (PARTITION p1 VALUES less than(5), PARTITION p2 VALUES less than(10), PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO Z1CASE VALUES ('ABCabc',11,00);
INSERT INTO Z1CASE VALUES ('123456',10,0.11);
INSERT INTO Z1CASE VALUES ('!@#123$%^&*',9,1.11);
INSERT INTO Z1CASE VALUES ('中文字符123',8,2.22);
INSERT INTO Z1CASE VALUES ('  ',7,3.33);
INSERT INTO Z1CASE VALUES ('',6,4.44);
INSERT INTO Z1CASE VALUES ('abcabc',5,5.555);
INSERT INTO Z1CASE VALUES ('11111.1111',4,6.666);
INSERT INTO Z1CASE VALUES ('!@#$%^&*',3,7.777);
INSERT INTO Z1CASE VALUES ('abc中文 字符123',2,8.888);
INSERT INTO Z1CASE VALUES (' ',1,9.999);
INSERT INTO Z1CASE VALUES ('',0,111.111);

--disable_warnings
--error 0,00942
drop table grouping1;
--enable_warnings
CREATE TABLE grouping1 (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;
INSERT INTO grouping1 VALUES (1, 868548608, 0, to_date('2008-12-25 01:10:47','yyyy-mm-dd hh24:mi:ss'), '', 0);
INSERT INTO grouping1 VALUES (2, 1387003904, 538902528, to_date('2004-02-15 10:44:52','yyyy-mm-dd hh24:mi:ss'), 'rmmjkovqnoqh', -2114322432);
INSERT INTO grouping1 VALUES (3, -1678966784, 0, (Timestamp '2009-04-06 17:15:12.047025'), NULL, 1865613312);

--disable_warnings
--error 0,00942
drop table wf_test1;
--enable_warnings
CREATE TABLE wf_test1 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;
insert into wf_test1(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1611857920, -474021888, -566689792, NULL, 1, 'this', to_date('2005-02-05 21:11:43','yyyy-mm-dd hh24:mi:ss'), to_date('2009-07-27','yyyy-mm-dd'), 1225785344);
insert into wf_test1(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1588789248, 749535232, 351666176, '', 2, 's', to_date('2006-06-23 06:15:53','yyyy-mm-dd hh24:mi:ss'), to_date('2007-06-26','yyyy-mm-dd'), 0);

--disable_warnings
--error 0,00942
drop table grouping11;
--enable_warnings
CREATE TABLE grouping11(
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) PARTITION BY hash(pk) partitions 4;

--disable_warnings
--error 0,00942
drop table cume_group;
--enable_warnings
create table cume_group (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
primary key (pk) ) partition by hash(pk) partitions 4;
insert into cume_group values (1, 868548608, 0, to_date('2008-12-25 01:10:47','yyyy-mm-dd hh24:mi:ss'), '', 0) ;
insert into cume_group values (2, 1387003904, 538902528, to_date('2004-02-15 10:44:52','yyyy-mm-dd hh24:mi:ss'), 'rmmjkovqnoqh', -2114322432) ;
insert into cume_group values (3, -1678966784, 0, (timestamp '2009-04-06 17:15:12.047025'), null, 1865613312) ;
insert into cume_group values (4, 1488257024, 782434304, to_date('2006-08-12 16:39:33','yyyy-mm-dd hh24:mi:ss'), 'i', -1414922240) ;
insert into cume_group values (5, 1508835328, 185335808, (timestamp '2002-07-12 19:18:28.007325'), 'x', 2096758784) ;
insert into cume_group values (6, -1587281920, 105971712, (timestamp '2001-12-19 06:21:34.036297'), 'so', 445972480) ;
insert into cume_group values (7, 167575552, -752877568, (timestamp '2006-11-22 15:47:47.010215'), 'm', 1246363648) ;
insert into cume_group values (8, -1590165504, -970457088, to_date('2008-04-16 03:59:05','yyyy-mm-dd hh24:mi:ss'), '', -429522944) ;
insert into cume_group values (9, -513409024, -803995648, (timestamp '2005-12-22 03:52:43.035922'), 'j', 0) ;
insert into cume_group values (10, -1531445248, 0, to_date('2001-08-15 21:22:47','yyyy-mm-dd hh24:mi:ss'), '', 0) ;
insert into cume_group values (11, -1583284224, 680329216, (timestamp '2009-12-23 19:40:09.048862'), 'z', 0) ;
insert into cume_group values (12, -582352896, -1330446336, (timestamp '2005-09-12 16:35:00.027314'), '', -391118848) ;
insert into cume_group values (13, 1471021056, 0, to_date('2008-04-11 02:33:14','yyyy-mm-dd hh24:mi:ss'), null, 398786560) ;
insert into cume_group values (14, -160432128, 622526464, to_date('2008-10-19','yyyy-mm-dd'), null, 303431680) ;
insert into cume_group values (15, 0, 942473216, null, '', 1044905984);
select pk,ceil(cumedistvalue) cumedistvalue,count(*) from (select pk,cume_dist() over (order by grouping(col_varchar2_20),grouping(pk) desc nulls last) cumedistvalue from cume_group where (COL_INT between 300 and 887266517765 and COL_DECIMAL_20_2 = 522654.38) or COL_DATE is NULL and abs(COL_NUMBER) <> 9999999 group by rollup(pk,col_varchar2_20) order by pk asc) group by pk,ceil(cumedistvalue);
select first_value (pk+col_int ignore nulls) over (partition by col_int order by col_int,pk desc nulls first) firstvalue from cume_group where (to_char(COL_DATE,'yyyymmdd') != 20191219 and COL_INT between 300 and 887266517765) or PK < 522654.38 and PK is NULL union select row_number() over (order by col_int,pk asc nulls last) rnumvalue from cume_group where (to_char(COL_DATE,'yyyymmdd') != 20191219 and COL_INT between 300 and 887266517765) or PK < 522654.38 and PK is NULL order by 1;

--disable_warnings
--error 0,00942
drop table grouping_rollup_t00;
--enable_warnings
create table grouping_rollup_t00 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) partition by hash(pk) partitions 4;
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1611857920, -474021888, -566689792, NULL, 1, 'this', to_date('2005-02-05 21:11:43','yyyy-mm-dd hh24:mi:ss'), to_date('2009-07-27','yyyy-mm-dd'), 1225785344);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1588789248, 749535232, 351666176, '', 2, 's', to_date('2006-06-23 06:15:53','yyyy-mm-dd hh24:mi:ss'), to_date('2007-06-26','yyyy-mm-dd'), 0);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1103495168, 384565248, 1958608896, '', 3, NULL, (Timestamp '2006-03-05 20:18:36.015514'), (Timestamp '2003-09-10 04:13:09.010524'), 844300288);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, 0, 0, 'q', 4, 'lhit', to_date('2001-11-10','yyyy-mm-dd'), (Timestamp '2000-02-10 21:45:18.021280'), -1576271872);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1646985216, 0, 0, 'm', 5, '', to_date('2004-09-25','yyyy-mm-dd'), (Timestamp '2007-05-16 19:15:59.034385'), 992542720);
insert into grouping_rollup_t00(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values('', '', '', '', 6, '', '', '', '');

--disable_warnings
--error 0,00942
drop table grouping_rollup_t01;
--enable_warnings
create table grouping_rollup_t01 (
col_int int,
col_decimal_20_2 decimal(20,2),
col_number number,
col_varchar2_20 varchar2(20),
pk smallint,
col_varchar2_21 varchar2(21),
col_date date,
col_timestamp timestamp,
col_decimal_21_2 decimal(21,2),
primary key (pk) ) partition by hash(pk) partitions 10;
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, 0, 0, '', 8, '', NULL, (Timestamp '2004-03-12 21:25:48.044571'), 1668415488);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(1455292416, -476250112, -1987248128, NULL, 9, 'she', to_date('2008-10-19','yyyy-mm-dd'), (Timestamp '2008-07-28 19:09:49.001817'), 798556160);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-2106064896, 148242432, -430309376, 'j', 10, 'rv', (Timestamp '2005-03-03 16:34:52.001480'), (Timestamp '2004-11-23 02:21:32.018522'), 0);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(0, -1016922112, 1504968704, 'w', 11, NULL, to_date('2007-04-21 16:13:18','yyyy-mm-dd hh24:mi:ss'), (Timestamp '2008-01-28 16:47:00.029273'), -718667776);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-759234560, 1424949248, -798031872, 'srvlhitckzgppy', 12, 'w', NULL, (Timestamp '2004-07-26 22:05:43.046113'), 0);
insert into grouping_rollup_t01(col_int,col_decimal_20_2,col_number,col_varchar2_20,pk,col_varchar2_21,col_date,col_timestamp,col_decimal_21_2) values(-1652883456, 530972672, 1590362112, 'are', 13, NULL, to_date('2008-07-01','yyyy-mm-dd'), (Timestamp '2005-12-22 07:38:05.023325'), 0);

--disable_warnings
--error 0,00942
drop table first_last_value1;
--enable_warnings
create table first_last_value1(c0 number,c1 number,c2 varchar2(10),c3 varchar2(10),c4 date,c5 number generated always as (c1+c0)) partition by hash(c1) partitions 4;
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,1,'oraman','dba','2019-12-10');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,2,'ross','sa','2019-12-11');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,3,'james','sa','2019-12-11');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,4,'terry','sa','2019-12-13');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,5,'cattiny','sa','2019-12-14');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,8,'terry','cto','2019-12-12');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,10,'gc','dba','2019-12-10');
insert into first_last_value1(c0,c1,c2,c3,c4) values(2,11,'henry','dba','2019-12-10');

--disable_warnings
--error 0,00942
drop table lead_lag2;
--enable_warnings
create table lead_lag2(c1 varchar2(64) not null,c2 varchar2(10) not null,c3 date not null, primary key (c1));
insert into lead_lag2 values ('1000001', 'AB7477', to_date('2015-11-30 10:18:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000002', 'AB7477', to_date('2015-11-30 10:22:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000003', 'AB7477', to_date('2015-11-30 10:28:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000004', 'AB7477', to_date('2015-11-30 10:29:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000005', 'AB7477', to_date('2015-11-30 10:39:13','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000006', 'AB7477', to_date('2015-11-30 10:45:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000007', 'AB7477', to_date('2015-11-30 10:56:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000008', 'AB7477', to_date('2015-11-30 10:57:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000009', 'AB3808', to_date('2015-11-30 11:00:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000010', 'AB3808', to_date('2015-11-30 11:10:13','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000011', 'AB3808', to_date('2015-11-30 11:15:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000012', 'AB3808', to_date('2015-11-30 11:26:12','YYYY-MM-DD HH24:mi:ss'));
insert into lead_lag2 values ('1000013', 'AB3808', to_date('2015-11-30 11:30:12','YYYY-MM-DD HH24:mi:ss'));

--disable_warnings
--error 0,00942
drop table wf_test111;
--enable_warnings
CREATE TABLE wf_test111 (
pk smallint,
col_number number,
col_int int,
col_date date,
col_varchar2_20 varchar2(20),
col_decimal_20_2 decimal(20,2),
/*Indices*/
primary key (pk));
insert INTO wf_test111 VALUES (1, 868548608, 0, (Timestamp '2008-12-25 01:10:47.050179'), 'there', 1387003904) ;
insert INTO wf_test111 VALUES (2, 538902528, 0, (Timestamp '2002-11-05 16:28:11.056788'), 'q', -475725824) ;
insert INTO wf_test111 VALUES (3, -1287520256, -361234432, (Timestamp '2000-07-20 05:30:47.051125'), 'back', -778567680) ;
insert INTO wf_test111 VALUES (4, -1103888384, -1961033728, (Timestamp '2004-03-07 14:02:48.037340'), 'w', -98959360) ;
insert INTO wf_test111 VALUES (5, 1601961984, -608960512, (Timestamp '2009-01-17 03:03:49.050687'), 'd', -868155392) ;
insert INTO wf_test111 VALUES (6, 1662976000, 0, (Timestamp '2001-11-28 00:50:27.051028'), 'the', -1465647104) ;
insert INTO wf_test111 VALUES (7, 0, 0, to_date('2002-11-01 20:02:37','yyyy-mm-dd hh24:mi:ss'), 'some', -298057728) ;
insert INTO wf_test111 VALUES (8, 1550123008, 2053308416, (Timestamp '2004-05-07 10:00:03.039599'), 'be', -1808990208) ;
COMMIT;