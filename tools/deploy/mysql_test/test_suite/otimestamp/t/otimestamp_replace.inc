
let $cnt=1;
while ($cnt <= 3)
{
  let $idx=0;
  while ($idx <= 10)
  {
    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str01);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str02);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str03);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str04);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str05);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str06);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str07);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str08);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str09);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str010);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str011);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str012);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str013);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str014);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str015);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str016);
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str11);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str110);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str114);
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str21);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str210);
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str214);
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str01), (sjhseq.nextval, $time_str02), (sjhseq.nextval, $time_str03), (sjhseq.nextval, $time_str04), (sjhseq.nextval, $time_str05), (sjhseq.nextval, $time_str06), (sjhseq.nextval, $time_str07), (sjhseq.nextval, $time_str08), (sjhseq.nextval, $time_str09), (sjhseq.nextval, $time_str010), (sjhseq.nextval, $time_str011), (sjhseq.nextval, $time_str012), (sjhseq.nextval, $time_str013), (sjhseq.nextval, $time_str014), (sjhseq.nextval, $time_str015), (sjhseq.nextval, $time_str016);
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str11), (sjhseq.nextval, $time_str12), (sjhseq.nextval, $time_str13), (sjhseq.nextval, $time_str14), (sjhseq.nextval, $time_str15), (sjhseq.nextval, $time_str16), (sjhseq.nextval, $time_str17), (sjhseq.nextval, $time_str18), (sjhseq.nextval, $time_str19), (sjhseq.nextval, $time_str110), (sjhseq.nextval, $time_str111), (sjhseq.nextval, $time_str112), (sjhseq.nextval, $time_str113), (sjhseq.nextval, $time_str114), (sjhseq.nextval, $time_str115), (sjhseq.nextval, $time_str116);
    eval  select c1 from t$cnt$idx order by id;

    eval  delete from  t$cnt$idx;
    eval  replace into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str21), (sjhseq.nextval, $time_str22), (sjhseq.nextval, $time_str23), (sjhseq.nextval, $time_str24), (sjhseq.nextval, $time_str25), (sjhseq.nextval, $time_str26), (sjhseq.nextval, $time_str27), (sjhseq.nextval, $time_str28), (sjhseq.nextval, $time_str29), (sjhseq.nextval, $time_str210), (sjhseq.nextval, $time_str211), (sjhseq.nextval, $time_str212), (sjhseq.nextval, $time_str213), (sjhseq.nextval, $time_str214), (sjhseq.nextval, $time_str215), (sjhseq.nextval, $time_str216);
    eval  select c1 from t$cnt$idx order by id;


    --error 1821
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str101);
    --error 1861
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str102);
    --error 1857
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str103);
    --error 1857
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str104);
    --error 1878
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str105);
    --error 1878
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str106);
    --error 1821
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str107);
    --error 1861
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str108);
    --error 1861
    eval  replace into t$cnt$idx (id, c1) values(sjhseq.nextval, $time_str109);
    --error 1861
    eval  replace into t$cnt$idx (id, c1) values (sjhseq.nextval, $time_str11), (sjhseq.nextval, $time_str12), (sjhseq.nextval, $time_str13), (sjhseq.nextval, $time_str14), (sjhseq.nextval, $time_str05), (sjhseq.nextval, $time_str102);
    inc $idx;
  }

  inc $cnt;
}


eval  delete from  t40;
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str01);
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str11);
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str21);
eval  select c1 from t40 order by id;

--error 1821
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str101);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str102);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str103);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str104);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str105);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str106);
--error 1830
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str107);
--error 0
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str108);
--error 1861
eval  replace into t40 (id, c1) values(sjhseq.nextval, $time_str109);
eval  select c1 from t40 order by id;
