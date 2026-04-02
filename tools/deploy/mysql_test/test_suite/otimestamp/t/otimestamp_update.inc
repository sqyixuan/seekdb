
let $cnt=1;
while ($cnt <= 3)
{
  let $idx=0;
  while ($idx <= 10)
  {
    select @@time_zone from dual;
    eval  delete from  t$cnt$idx;
    eval  insert into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str01), (sjhseq.nextval, $time_str11), (sjhseq.nextval, $time_str21);
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str01;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str02;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str03;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str04;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str05;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str06;
    eval  select c1 from t$cnt$idx order by id;
    
    if ($cnt == 1) {
      eval  update t$cnt$idx set c1 = $time_str07;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str08;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str09;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str010;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str011;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str012;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str013;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str014;
      eval  select c1 from t$cnt$idx order by id;
    }

    eval  update t$cnt$idx set c1 = $time_str11;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str12;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str13;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str14;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str15;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str16;
    eval  select c1 from t$cnt$idx order by id;
    
    if ($cnt == 1) {
      eval  update t$cnt$idx set c1 = $time_str17;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str18;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str19;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str110;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str111;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str112;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str113;
      eval  select c1 from t$cnt$idx order by id;

      eval  update t$cnt$idx set c1 = $time_str114;
      eval  select c1 from t$cnt$idx order by id;
    }

    eval  update t$cnt$idx set c1 = $time_str21;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str22;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str23;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str24;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str25;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str26;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str27;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str28;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str29;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str210;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str211;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str212;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str213;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str214;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str215;
    eval  select c1 from t$cnt$idx order by id;

    eval  update t$cnt$idx set c1 = $time_str216;
    eval  select c1 from t$cnt$idx order by id;

    --error 1858
    eval  update t$cnt$idx set c1 = $time_str101;
    if ($cnt == 1) {
      --error 1861
      eval  update t$cnt$idx set c1 = $time_str102;
      --error 1857
      eval  update t$cnt$idx set c1 = $time_str103;
      --error 1857
      eval  update t$cnt$idx set c1 = $time_str104;
      --error 1878
      eval  update t$cnt$idx set c1 = $time_str105;
      --error 1878
      eval  update t$cnt$idx set c1 = $time_str106;
    }
    if ($cnt != 1) {
      --error 1830
      eval  update t$cnt$idx set c1 = $time_str107;
    }
    --error 1841
    eval  update t$cnt$idx set c1 = $time_str108;
    inc $idx;
  }

  inc $cnt;
}




eval  delete from  t40;
eval  insert into t40 (id, c1) values (sjhseq.nextval, Timestamp $time_str01), (sjhseq.nextval, Timestamp $time_str11), (sjhseq.nextval, $time_str21);
eval  select c1 from t40 order by id;

eval  update t40 set c1 = Timestamp $time_str01;
eval  select c1 from t40 order by id;

eval  update t40 set c1 = Timestamp $time_str11;
eval  select c1 from t40 order by id;

eval  update t40 set c1 = $time_str21;
eval  select c1 from t40 order by id;


--error 1858
eval  update t40 set c1 = $time_str101;
if ($cnt == 1) {
  --error 1861
  eval  update t$cnt$idx set c1 = $time_str102;
  --error 1857
  eval  update t40 set c1 = $time_str103;
  --error 1857
  eval  update t40 set c1 = $time_str104;
  --error 1878
  eval  update t40 set c1 = $time_str105;
  --error 1857
  eval  update t40 set c1 = $time_str106;
}
--error 1830
eval  update t40 set c1 = $time_str107;
--error 1841
eval  update t40 set c1 = $time_str108;
--error 1841
eval  update t40 set c1 = $time_str109;
eval  select c1 from t40 order by id;
