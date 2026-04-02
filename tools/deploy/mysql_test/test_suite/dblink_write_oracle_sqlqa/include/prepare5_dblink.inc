ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0,942
DROP TABLE test1 purge;
--error 0,942
DROP TABLE test2 purge;
--error 0,942
DROP TABLE test3 purge;
--enable_warnings

--echo #sub-6-1	普通表+无索引+无主键

CREATE TABLE test1(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);
INSERT INTO test1 VALUES(101, 'test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test1 VALUES(102, 'test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test1 VALUES(103, 'test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test1 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test1 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test1 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test1 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test1 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test2(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);
INSERT INTO test2 VALUES(101, 'test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test2 VALUES(102, 'test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test2 VALUES(103, 'test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test2 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test2 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test2 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test2 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test2 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test3(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);


--error 0,942
drop table DemoOrganization purge;
create table DemoOrganization 
(
OrgID     NUMBER(20,0) not null,
OrgName   NVARCHAR2(10),
OrgPath   VARCHAR2(10),
ParentID  INTEGER,
OLevel    INTEGER default 0,
AddTime   DATE);
--error 0,2289
drop sequence SEQ_DEMOORGANIZATION;
create sequence SEQ_DEMOORGANIZATION minvalue 1 maxvalue 999999999999999 start with 1 increment by 1 cache 201;
delimiter //;
create or replace trigger TRI_SEQ_DEMOORGANIZATION
before insert on DEMOORGANIZATION
for each row
begin
select SEQ_DEMOORGANIZATION.NEXTVAL into:new.ORGID from dual;
end;
//
delimiter ;//
DELETE FROM DEMOORGANIZATION;
INSERT INTO DEMOORGANIZATION(ORGNAME,OLEVEL,ORGPATH,PARENTID ,ADDTIME)
select '组织机构1',1,'0',0,to_date('19900101','yyyymmdd') from dual union all
select '组织机构2',1,'0',0,to_date('20000101','yyyymmdd') from dual union all
select '组织机构3',1,'0',0,to_date('20100101','yyyymmdd') from dual union all
select '组织机构4',2,'1',1,to_date('20200101','yyyymmdd') from dual union all
select '组织机构5',2,'2',2,to_date('20300101','yyyymmdd') from dual union all
select '组织机构6',3,'1/4',4,to_date('20400101','yyyymmdd') from dual union all
select '组织机构7',3,'1/4',4,to_date('20500101','yyyymmdd') from dual union all
select '组织机构8',3,'2/5',5,to_date('20600101','yyyymmdd') from dual union all
select '组织机构9',4,'1/4/6',6,to_date('20700101','yyyymmdd') from dual union all
select '组织机构10',4,'1/4/6',6,to_date('20800101','yyyymmdd') from dual union all
select '组织机构11',4,'1/4/6',6,to_date('20900101','yyyymmdd') from dual union all
select '组织机构12',4,'2/5/8',8,to_date('21000101','yyyymmdd') from dual union all
select '组织机构13',4,'2/5/8',8,to_date('21100101','yyyymmdd') from dual;
drop trigger TRI_SEQ_DEMOORGANIZATION;



--echo #sub-6-4	分区表+全局非分区索引+有主键
--echo #主键为复合主键
--error 0,942
DROP TABLE test111 purge;
--error 0,942
DROP TABLE test222 purge;
--error 0,942
DROP TABLE test333 purge;
CREATE TABLE test111(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday))
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_11222 ON test111(id) GLOBAL;
CREATE INDEX idx_12222 ON test111(name) GLOBAL;
INSERT INTO test111 VALUES(101, 'test111', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test111 VALUES(102, 'test222', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test111 VALUES(103, 'test333', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test111 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test111 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test111 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test111 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test111 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test222(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday))
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_21222 ON test222(id) GLOBAL;
CREATE INDEX idx_22222 ON test222(name) GLOBAL;
INSERT INTO test222 VALUES(101, 'test111', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test222 VALUES(102, 'test222', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test222 VALUES(103, 'test333', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test222 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test222 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test222 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test222 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test222 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test333(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday))
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_31222 ON test333(id) GLOBAL;
CREATE INDEX idx_32222 ON test333(name) GLOBAL;

COMMIT;