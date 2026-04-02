desc t1;

insert into t1 (c1) values (1);
if ($is_tz == 0)
{
  if ($is_const_default == 1)
  {
    select c3 from t1;
  }
  if ($is_const_default == 0)
  {
    if ($scale == 0)
    {
      if ($is_date_type == 0)
      {
        --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]./timestamp is valid/g
        select c3 from t1;
      }
      if ($is_date_type == 1)
      {
        --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]/date is valid/g
        select c3 from t1;
      }
    }
    if ($scale == 1)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{1}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 2)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{2}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 3)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{3}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 4)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{4}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 5)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{5}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 6)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{6}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 7)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{7}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 8)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{8}/timestamp is valid/g
      select c3 from t1;
    }
    if ($scale == 9)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{9}/timestamp is valid/g
      select c3 from t1;
    }
  }
}


if ($is_tz == 1)
{
  if ($is_const_default == 1)
  {
    select c3 from t1;
  }
  if ($is_const_default == 0)
  {
    if ($scale == 0)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]. \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 1)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{1} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 2)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{2} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 3)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{3} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 4)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{4} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 5)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{5} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 6)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{6} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 7)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{7} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 8)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{8} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
    if ($scale == 9)
    {
      --replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{9} \+08:00/timestamp tz is valid/g
      select c3 from t1;
    }
  }
}

alter table t1 add c4 date default '2012-01-01 12:00:12';
select c4 from t1;

alter table t1 add c5 timestamp default '2012-01-01 12:00:00.123';
select c5 from t1;

alter table t1 add c6 date default sysdate;
--replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]/date is valid/g
select c6 from t1;

alter table t1 add c7 timestamp default systimestamp;
--replace_regex /20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{6}/timestamp is valid/g
select c7 from t1;

drop table t1;
--echo -----------------------------------------
