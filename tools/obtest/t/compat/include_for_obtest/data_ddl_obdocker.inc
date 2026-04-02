#connection tentsql_conn;
#let $cnt=1;
#while($cnt <= 50)
#{
#  eval create database testsvr_$cnt;
#  eval use testsvr_$cnt;
#  eval create table t3(c1 int,c2 varchar(65536),c3 varchar(65536),c4 varchar(65536));
#  eval insert into t3 (c1,c2,c3,c4) values($cnt,"c2","c3","c4");
#  eval update t3 set c2="cc" where c1=2;
#  eval delete from t3 where c1=9;
#  inc $cnt;
#}

####oracle租户插入数据
let $cnt=2;
while($cnt <= 50)
{
   connection tentora_conn;
   eval create table t2_$cnt(c1 int, c2 int, c3 int, primary key(c1));
   eval insert into t2_$cnt values(1, 5, 1), (2, 4, 2), (3, 3, 3), (4, 2, 4), (5, 1, 5),(6,2,6),(7,5,7),(8,2,8),(9,3,9);
   eval update t2_$cnt set c2=22 where c1=2;
   eval delete from t2_$cnt where c1=9;
   inc $cnt;
}
