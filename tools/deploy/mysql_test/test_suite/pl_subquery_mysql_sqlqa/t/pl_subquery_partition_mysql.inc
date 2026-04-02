
delimiter //;

--echo ob_pl_subquery_2_4
--echo #游标

drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal from t2 partition(p2) where sal > 3000;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a;
	if done != 1 then
	if (length(b)) = 4 and exists (select * from t2 partition(p2) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','success');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end if;
	end if;
	end while;
	close mc;
end;
//

call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal,job from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a,c;
	if done != 1 then
	if (c = 'CLERK') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','低');
	select msg;
	elseif (c = 'SALESMAN') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','一般');
	select msg;
	elseif (c = 'MANAGER') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','中');
	select msg;
	elseif (c = 'ANALYST') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','高');
	select msg;
	elseif (c = 'PRESIDENT') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','很高');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end if;
	end if;
	end while;
	close mc;
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal,job from t1 e inner join t2 partition(p1)p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a,c;
	if done != 1 then
	if (c = 'CLERK') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','低');
	select msg;
	elseif (c = 'SALESMAN') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','一般');
	select msg;
	elseif (c = 'MANAGER') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','中');
	select msg;
	elseif (c = 'ANALYST') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','高');
	select msg;
	elseif (c = 'PRESIDENT') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','很高');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end if;
	end if;
	end while;
	close mc;
end;
//

call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select ename,job,sal from t2 partition(p1) order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 5) and a >= all(select sal from t2 partition(p1) where job = c and ename = b) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 8) and a >=  some(select sal from t2 partition(p1) where job = c and ename = b) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 7) and a in (select sal from t2 partition(p1) where job like concat('%',c,'%') and ename like concat('%',b,'%')) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;
end;
//

call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select ename,job,sal from t2 partition(p1) order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 5) and a >= (select avg(sal) from t2 partition(p1) where job = c group by b) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 8) and a <= some(select max(sal) from t2 partition(p1) where job = c group by b having max(sal) > a) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 7) and not exists (select sal from t2 partition(p1) where job like concat('%',c,'%') and ename like concat('%',b,'%')) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;
end;
//

--echo #38076059- OB error while PL subquery nested GREOUP BY
--error 0,4016
call p1();//


--echo ob_pl_subquery_1_2
--echo #连接谓词
drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
    declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and exists(select * from t1 e right join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c)  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;
end;
//

call p1();//



drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
    declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and exists(select * from t1 e natural join t2 partition(p1) p  where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and exists(select * from t1 e cross join t2 partition(p1) p where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c union all select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c)  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;
end;
//

call p1();//


--echo ob_pl_subquery_1_3
--echo #过滤谓词

drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and not exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c and p.mgr is null and p.job is not null) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc != (select * from (select loc from t1 where loc != c order by 1)e limit 1)) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = all(select sal from t2 partition(p1) where sal = a) and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal >= any(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c ) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where  p.sal <= some(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal != all(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//




drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) or not exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c and p.mgr is null and p.job is not null) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc != (select * from (select loc from t1 where loc != c order by 1)e limit 1)) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = all(select sal from t2 partition(p1) where sal = a) and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) or exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal >= any(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c ) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where  p.sal <= some(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal != all(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//




drop procedure if exists p1;//
 
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and a in (select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and a not in (select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc != c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and a between (select min(p.sal) from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) and  (select max(p.sal) from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	if (length(c) = 6) and (select e.loc from t1 e, t2 partition(p1) p where e.deptno = p.deptno and p.ename = b ) is not null or (select p.mgr from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.sal = a) is null then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	elseif (length(c) = 7) and (select e.loc from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.sal = a) like concat('%',c,'%') and (select e.loc from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.ename = b limit 1) not like concat('%',c,'%')  then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	elseif (length(c) = 8) and a >= all(select sal from t2 partition(p1) where ename = b) and a >= any(select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end if;
    end if;
	end while;
	close mc;   
end;
//

call p1();//




--echo ob_pl_subquery_2_1_2
--echo #case

drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
    while done != 1 do
    set msg = '';
   	fetch mc into b,c,a;
   	if done != 1 then
	case 
	when (length(c)) = 6  and not exists (select * from t1 e where e.loc = c) then set msg = concat(a,' ',b,' ','低'); select msg;
	when (length(c)) = 7  and exists (select * from t1 e where e.loc = c) then set msg = concat(a,' ',b,' ','一般'); select msg;
	when (length(c)) = 8  and exists (select * from t1 e where e.loc = c) then set msg = concat(a,' ',b,' ','中'); select msg;
	else set msg = concat(msg,b,'error'); select msg;
	end case;
	end if;
	end while;
	close mc;
end;
//


call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal from t2 partition(p2) where sal > 2000;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a;
	if done != 1 then
	case when (length(b)) = 4 and exists (select * from t2 partition(p2) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','success');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end case;
	end if;
	end while;
	close mc;
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal,job from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a,c;
	if done != 1 then
	case when (c = 'CLERK') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','低');
	select msg;
	when (c = 'SALESMAN') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','一般');
	select msg;
	when (c = 'MANAGER') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','中');
	select msg;
	when (c = 'ANALYST') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','高');
	select msg;
	when (c = 'PRESIDENT') and exists (select * from t2 partition(p1) where ename = b and job = c) then
	set msg = concat((select b),' ',(select a),' ','很高');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end case;
	end if;
	end while;
	close mc;
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal,job from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,a,c;
	if done != 1 then
	case when (c = 'CLERK') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','低');
	select msg;
	when (c = 'SALESMAN') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','一般');
	select msg;
	when (c = 'MANAGER') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','中');
	select msg;
	when (c = 'ANALYST') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','高');
	select msg;
	when (c = 'PRESIDENT') and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','很高');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end case;
	end if;
	end while;
	close mc;
end;
//


call p1();//




drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select ename,job,sal from t2 partition(p1) order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 5) and a >= all(select sal from t2 partition(p1) where job = c and ename = b) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 8) and a >=  some(select sal from t2 partition(p1) where job = c and ename = b) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 7) and a in (select sal from t2 partition(p1) where job like concat('%',c,'%') and ename like concat('%',b,'%')) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;
end;
//


call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select ename,job,sal from t2 partition(p1) order by 1,2,3;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 5) and a >= (select avg(sal) from t2 partition(p1) where job = c group by b) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 8) and a <= some(select max(sal) from t2 partition(p1) where job = c group by b having max(sal) > a) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 7) and not exists (select sal from t2 partition(p1) where job like concat('%',c,'%') and ename like concat('%',b,'%')) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;
end;
//

--echo #38076059- OB error while PL subquery nested GREOUP BY
--error 0,4016
call p1();//


--echo ob_pl_subquery_1_2
--echo #连接谓词
drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
    declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and exists(select * from t1 e right join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c)  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;
end;
//


call p1();//



drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
    declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and exists(select * from t1 e natural join t2 partition(p1) p  where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and exists(select * from t1 e cross join t2 partition(p1) p where p.sal = a and p.ename = b and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c union all select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c)  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;
end;
//


call p1();//


--echo ob_pl_subquery_1_3
--echo #过滤谓词

drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and not exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c and p.mgr is null and p.job is not null) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc != (select * from (select loc from t1 where loc != c order by 1)e limit 1)) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = all(select sal from t2 partition(p1) where sal = a) and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal >= any(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c ) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where  p.sal <= some(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal != all(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e left join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename not in (select ename from t2 partition(p1) where ename = b) and e.loc in (select loc from t1 where loc != c)) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) or not exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and not exists(select * from t2 partition(p1) where ename = b) and exists(select * from t1 where loc = c )) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal between a and 5000 and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) or not exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc = c and p.mgr is null and p.job is not null) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = a and p.ename = b and e.loc != (select * from (select loc from t1 where loc != c order by 1)e limit 1)) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal = all(select sal from t2 partition(p1) where sal = a) and p.ename = b and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) or exists(select * from t1 e left join t2 partition(p1) p on e.deptno = p.deptno where p.sal >= any(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c ) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where  p.sal <= some(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) or exists(select * from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno where p.sal != all(select sal from t2 partition(p1) where sal = a and ename = b) and e.loc = c )  then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//




drop procedure if exists p1;//
 
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and a in (select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and a not in (select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc != c) then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and a between (select min(p.sal) from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) and  (select max(p.sal) from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//

call p1();//


drop procedure if exists p1;//

create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare c varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0 ;
	declare mc cursor for  select p.ename,e.loc,p.sal from t1 e inner join t2 partition(p1) p on e.deptno = p.deptno order by 1,2,3;
	declare continue handler for not found set done = 1;	
    open mc;
	while done != 1 do
	set msg = '';
	fetch mc into b,c,a;
	if done != 1 then
	case when (length(c) = 6) and (select e.loc from t1 e, t2 partition(p1) p where e.deptno = p.deptno and p.ename = b ) is not null or (select p.mgr from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.sal = a) is null then
	set msg = concat(a,' ',b,' ','低');
	select msg;
	when (length(c) = 7) and (select e.loc from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.sal = a) like concat('%',c,'%') and (select e.loc from t1 e,t2 partition(p1) p where e.deptno = p.deptno and p.ename = b limit 1) not like concat('%',c,'%')  then
	set msg = concat(a,' ',b,' ','一般');
	select msg;	
	when (length(c) = 8) and a >= all(select sal from t2 partition(p1) where ename = b) and a >= any(select p.sal from t1 e,t2 partition(p1) p where e.deptno = p.deptno and e.loc = c) then
	set msg = concat(a,' ',b,' ','中');
	select msg;	
	else
	set msg = concat(msg,b,'error');
	select msg;
	end case;
    end if;
	end while;
	close mc;   
end;
//


call p1();//



drop procedure if exists p1;//
create procedure p1()
begin
	declare a int;
	declare b varchar(50) character set utf8;
	declare msg varchar(50) character set utf8;
	declare done int default 0;
	declare mc cursor for select ename,sal from t2 partition(p2) where sal > 2000;
	declare continue handler for not found set done = 1;	
	open mc;
	while done != 1 and exists(select * from t2 partition(p1) where sal < 2000) do
	set msg = '';
	fetch mc into b,a;
	if done != 1 then
	if (length(b)) = 4 and exists (select * from t2 partition(p1) where ename = b and sal = a) then
	set msg = concat((select b),' ',(select a),' ','success');
	select msg;
	else 
	set msg = concat(msg,b,'error');
	select msg;
	end if;
	end if;
	end while;
	close mc;
end;
//

call p1();//

delimiter ;//

drop table if exists t1;
drop table if exists t2;

