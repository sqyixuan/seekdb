#select
create outline olt_1 on select /*+index(t1 c3) lalalala*/ c2,c1 from t1 where c2 = 2;
create outline olt_2 on select /*+index(t1 c3) lalalala*/ c2,c1 from t1 where c2 = 2 order by 1;
create outline olt_3 on select /*+INDEX(@"SEL$2" "t1"@"SEL$2" "c3")*/* from (select c2,c1 from t1 where c1 >1) as tt;
create outline olt_4 on select /*+index(t1 c3) trace_log*/c2,c1 from t1 where c1 < any(select c1 from t2 where c1 >1);
