
let $cnt=1;
while ($cnt <= 4)
{
  let $is_tz=0;
  let $is_date=0;
  if ($cnt == 1)
  {
    let $is_tz = 1;
  }
  if ($cnt == 4)
  {
    let $is_date = 1;
  }

  let $idx=0;
  while ($idx <= 1)
  {
    select @@time_zone from dual;
    eval  delete from  t$cnt$idx;

    --echo test error str
    --error 1841
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '0000-01-01 01:02:03');
    --error 1858
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '-0001-01-01 01:02:03');
    --error 1861
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '10000-01-01 01:02:03');
    --error 1847
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-00-01 01:02:03');
    --error 1861
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-13-01 01:02:03');
    --error 1847
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-01-00 01:02:03');
    --error 1847
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-01-32 01:02:03');
    --error 1861
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-02-29 01:02:03');
    --error 1852
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-01-02 01:02:60');
    --error 1851
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:60:03');
    --error 1850
    eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 24:02:03');

    if ($is_date == 0)
    {
      if ($cnt == 1)
      {
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.');
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.0123456789');
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.0123.456789');
      }
      if ($cnt != 1)
      {
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.');
        --error 1830
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.0123456789');
        --error 1830
        eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.0123.456789');
      }
    }

    if ($is_tz == 1)
    {
      --error 1861
      eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.012345678 -16:00');
      --error 1861
      eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.012345678 +16:00');
      --error 1861
      eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.012345678 +01:60');
      --error 1861
      eval  insert into t$cnt$idx (id, c1) values(sjhseq.nextval, '1970-10-01 01:02:03.012345678 -01:60');
    }
    inc $idx;

    if ($is_date == 1)
    {
      let $idx=11;
    }
  }
  inc $cnt;
}
