connection tentora_conn;

call p1(1);
begin p1(1); end;

#select * from t1;
#select * from t1 where c1 = f1(1);

call pkg1.p1(3);
select pkg1.f1(4) from dual;



#select println() from dual;



insert into t1 values(20, 20, 20);
select * from t1;
select * from t1_log;



select println() from dual;
#select println() from dual;


select * from tt2;


insert into t3 values(2, 2);
insert into t4 values(3, 3);
insert into t4 values (2, 2);
select * from t3;
select * from t4;


call p1(1);
select println() from dual;

call p1(2);
select println() from dual;

call p1(3);
select println() from dual;


select * from t1_1;


insert into t1_2 values(2);
insert into t1_2 values(60);

select * from t1_2;

select println() from dual;


set autocommit =0;
INSERT INTO accounts (account_id, balance)
VALUES (7765, 6350.00);
 
INSERT INTO accounts (account_id, balance)
VALUES (7760, 5100.50);

BEGIN transfer(7715, 7720, 250);END;

SELECT * FROM accounts;

#create table t_1(aa varchar2(2000));
#
#select * from t1_3;
#select * from t_1;
#
#select * from t1_4;
#select * from t_2;
