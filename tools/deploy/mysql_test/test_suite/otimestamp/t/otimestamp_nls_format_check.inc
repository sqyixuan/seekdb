
delete from t001;
eval insert into t001 values(1, $timestr1, $timestr1, $timestr1, $timestr1);
eval insert into t001 values(2, $timestr2, $timestr2, $timestr2, $timestr1);
eval insert into t001 values(3, $timestr3, $timestr2, $timestr2, $timestr1);
eval insert into t001 values(4, $timestr4, $timestr4, $timestr4, $timestr4);
eval insert into t001 values(5, $timestr5, $timestr5, $timestr5, $timestr4);
eval insert into t001 values(6, $timestr6, $timestr5, $timestr5, $timestr4);
select * from t001 order by id;
select id, to_char(c1), to_char(c2), to_char(c3), to_char(c4) from t001 order by id;
select id, to_timestamp_tz(to_char(c1)), to_timestamp(to_char(c2)), to_timestamp(to_char(c3)), to_date(to_char(c4)) from t001 order by id;

delete from t002;
insert into t002 select * from t001;
select * from t002 order by id;
select id, to_char(c1), to_char(c2), to_char(c3), to_char(c4) from t002 order by id;
select id, to_timestamp_tz(to_char(c1)), to_timestamp(to_char(c2)), to_timestamp(to_char(c3)), to_date(to_char(c4)) from t002 order by id;

