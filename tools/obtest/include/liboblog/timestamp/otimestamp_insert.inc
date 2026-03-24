
let $cnt=1;
while ($cnt <= 3)
{
  let $idx=0;
  while ($idx <= 10)
  {
    select @@time_zone from dual;

    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str01);
    ob1.oblog.next;
    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str02);
    ob1.oblog.next;
    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str03);
    ob1.oblog.next;
    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str04);
    ob1.oblog.next;
    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str05);
    ob1.oblog.next;
    eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str06);
    ob1.oblog.next;

    # 只有timestamp with time zone 才允许插入
    if ($cnt == 1)
    {
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str07);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str08);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str09);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str010);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str011);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str012);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str013);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str014);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str015);
      ob1.oblog.next;
      eval  insert into t$cnt$idx (id, c1) values(oblog_db_seq.nextval, $time_str016);
      ob1.oblog.next;
    }
    #eval  select c1 from t$cnt$idx order by id;

    eval  insert into t$cnt$idx (id, c1) values (oblog_db_seq.nextval, $time_str21), (oblog_db_seq.nextval, $time_str22), (oblog_db_seq.nextval, $time_str23), (oblog_db_seq.nextval, $time_str24), (oblog_db_seq.nextval, $time_str25), (oblog_db_seq.nextval, $time_str26);
    ob1.oblog.next;

    # 只有timestamp with time zone 才允许插入
    if ($cnt == 1)
    {
      eval  insert into t$cnt$idx (id, c1) values (oblog_db_seq.nextval, $time_str27), (oblog_db_seq.nextval, $time_str28), (oblog_db_seq.nextval, $time_str29), (oblog_db_seq.nextval, $time_str210), (oblog_db_seq.nextval, $time_str211), (oblog_db_seq.nextval, $time_str212), (oblog_db_seq.nextval, $time_str213), (oblog_db_seq.nextval, $time_str214), (oblog_db_seq.nextval, $time_str215), (oblog_db_seq.nextval, $time_str216);
      ob1.oblog.next;
    }

    # 清理表
    eval  delete from t$cnt$idx;
    ob1.oblog.next;

    inc $idx;
    inc $idx;
  }

  inc $cnt;
}
