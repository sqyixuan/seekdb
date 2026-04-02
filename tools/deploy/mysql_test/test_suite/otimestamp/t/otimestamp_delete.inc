
let $cnt=1;
while ($cnt <= 3)
{
  let $idx=0;
  while ($idx <= 10)
  {
    eval  delete from  t$cnt$idx;
    eval  insert into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str01), (sjhseq.nextval, $time_str02), (sjhseq.nextval, $time_str03), (sjhseq.nextval, $time_str04), (sjhseq.nextval, $time_str05), (sjhseq.nextval, $time_str06);
    if ($cnt == 1) {
      eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str07), (sjhseq.nextval, $time_str08), (sjhseq.nextval, $time_str09), (sjhseq.nextval, $time_str010), (sjhseq.nextval, $time_str011), (sjhseq.nextval, $time_str012), (sjhseq.nextval, $time_str013), (sjhseq.nextval, $time_str014), (sjhseq.nextval, $time_str015), (sjhseq.nextval, $time_str016);
    }
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from t$cnt$idx where c1 like '%00:00:00%';
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from t$cnt$idx where c1 > '1991-09-14 08:00:00.000000';
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from t$cnt$idx where c1 >= '1991-10-02 08:00:00';
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from t$cnt$idx;
    eval  select c1 from t$cnt$idx order by id;

    inc $idx;
  }

  inc $cnt;
}


eval  delete from  t40;
eval  insert into t40 (id, c1) values (sjhseq.nextval, Timestamp $time_str01), (sjhseq.nextval, Timestamp $time_str11), (sjhseq.nextval, $time_str21);
eval  select c1 from t40 order by id;

eval  delete from t40 where c1 like '%00:00:00%';
eval  select c1 from t40 order by id;

eval  delete from t40 where c1 > '1991-09-14 08:00:00';
eval  select c1 from t40 order by id;

eval  delete from t40 where c1 >= '1991-10-02 08:00:00';
eval  select c1 from t40 order by id;

eval  delete from t40;
eval  select c1 from t40 order by id;
