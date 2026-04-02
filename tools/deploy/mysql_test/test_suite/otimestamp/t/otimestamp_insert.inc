
--disable_warnings

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
  while ($idx <= 10)
  {
    eval delete from  t$cnt$idx;

    if ($is_date == 0)
    {
      --echo test 'yyyy-mm-dd hh:mi:ss.ns tzh:tzm'
      let $ymd_idx=1;
      while ($ymd_idx <= $ymd_ok_cnt)
      {
        let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
        let $hms_idx=1;
        while ($hms_idx <= $hms_ok_cnt)
        {
          let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
          let $ns_idx=1;
          while ($ns_idx <= $ns_ok_cnt)
          {
            let $ns_ok_str = query_get_value(select c1 from t_ns_ok where id=$ns_idx, C1, 1);
            if ($is_tz == 1)
            {
              let $tz_idx=1;
              while ($tz_idx <= $tz_ok_cnt)
              {
                let $tz_ok_str = query_get_value(select c1 from t_tz_ok where id=$tz_idx, C1, 1);
                eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, '$ymd_ok_str$hms_ok_str$ns_ok_str$tz_ok_str');
                inc $tz_idx;
              }
            }
            if ($is_tz == 0)
            {
              eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, '$ymd_ok_str$hms_ok_str$ns_ok_str');
            }
            inc $ns_idx;
          }
          inc $hms_idx;
        }
        inc $ymd_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;
    }


    if ($is_tz == 1)
    {
      --echo test 'yyyy-mm-dd hh:mi:ss.ns'
      let $ymd_idx=1;
      while ($ymd_idx <= $ymd_ok_cnt)
      {
        let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
        let $hms_idx=1;
        while ($hms_idx <= $hms_ok_cnt)
        {
          let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
          let $ns_idx=1;
          while ($ns_idx <= $ns_ok_cnt)
          {
            let $ns_ok_str = query_get_value(select c1 from t_ns_ok where id=$ns_idx, C1, 1);
            eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, '$ymd_ok_str$hms_ok_str$ns_ok_str');
            inc $ns_idx;
          }
          inc $hms_idx;
        }
        inc $ymd_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;
    }


    if ($is_tz == 0)
    {
      --echo test 'yyyy-mm-dd hh:mi:ss'
      let $ymd_idx=1;
      while ($ymd_idx <= $ymd_ok_cnt)
      {
        let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
        let $hms_idx=1;
        while ($hms_idx <= $hms_ok_cnt)
        {
          let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
          eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, '$ymd_ok_str$hms_ok_str');
          inc $hms_idx;
        }
        inc $ymd_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;


      --echo test 'yyyy-mm-dd'
      let $ymd_idx=1;
      while ($ymd_idx <= $ymd_ok_cnt)
      {
        let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
        eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, '$ymd_ok_str');
        inc $ymd_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;
    }


    --echo test Timestamp 'yyyy-mm-dd hh:mi:ss.ns tzh:tzm'
    let $ymd_idx=1;
    while ($ymd_idx <= $ymd_ok_cnt)
    {
      let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
      let $hms_idx=1;
      while ($hms_idx <= $hms_ok_cnt)
      {
        let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
        let $ns_idx=1;
        while ($ns_idx <= $ns_ok_cnt)
        {
          let $ns_ok_str = query_get_value(select c1 from t_ns_ok where id=$ns_idx, C1, 1);
          if ($is_tz == 1)
          {
            let $tz_idx=1;
            while ($tz_idx <= $tz_ok_cnt)
            {
              let $tz_ok_str = query_get_value(select c1 from t_tz2_ok where id=$tz_idx, C1, 1);
              eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, Timestamp '$ymd_ok_str$hms_ok_str$ns_ok_str$tz_ok_str');
              inc $tz_idx;
            }
          }
          if ($is_tz == 0)
          {
            eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, Timestamp '$ymd_ok_str$hms_ok_str$ns_ok_str');
          }
          inc $ns_idx;
        }
        inc $hms_idx;
      }
      inc $ymd_idx;
    }
    eval select c1 from t$cnt$idx order by id;
    eval delete from  t$cnt$idx;


    if ($is_tz == 1)
    {
      --echo test Timestamp 'yyyy-mm-dd hh:mi:ss.ns'
      let $ymd_idx=1;
      while ($ymd_idx <= $ymd_ok_cnt)
      {
        let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
        let $hms_idx=1;
        while ($hms_idx <= $hms_ok_cnt)
        {
          let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
          let $ns_idx=1;
          while ($ns_idx <= $ns_ok_cnt)
          {
            let $ns_ok_str = query_get_value(select c1 from t_ns_ok where id=$ns_idx, C1, 1);
            eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, Timestamp '$ymd_ok_str$hms_ok_str$ns_ok_str');
            inc $ns_idx;
          }
          inc $hms_idx;
        }
        inc $ymd_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;
    }


    --echo test Timestamp 'yyyy-mm-dd hh:mi:ss'
    let $ymd_idx=1;
    while ($ymd_idx <= $ymd_ok_cnt)
    {
      let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
      let $hms_idx=1;
      while ($hms_idx <= $hms_ok_cnt)
      {
        let $hms_ok_str = query_get_value(select c1 from t_hms_ok where id=$hms_idx, C1, 1);
        eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, Timestamp '$ymd_ok_str$hms_ok_str');
        inc $hms_idx;
      }
      inc $ymd_idx;
    }
    eval select c1 from t$cnt$idx order by id;
    eval delete from  t$cnt$idx;


    --echo test Date 'yyyy-mm-dd'
    let $ymd_idx=1;
    while ($ymd_idx <= $ymd_ok_cnt)
    {
      let $ymd_ok_str = query_get_value(select c1 from t_ymd_ok where id=$ymd_idx, C1, 1);
      eval insert into t$cnt$idx (id, c1) values (sjhseq.nextval, Date '$ymd_ok_str');
      inc $ymd_idx;
    }
    eval select c1 from t$cnt$idx order by id;
    eval delete from  t$cnt$idx;

    if ($is_tz == 1)
    {
      --echo test special str
      let $spe_idx=1;
      while ($spe_idx <= $spe_ok_cnt)
      {
        eval insert into t$cnt$idx (id, c1) select sjhseq.nextval, c1 from t_spe_ok where t_spe_ok.id=$spe_idx;
        inc $spe_idx;
      }
      eval select c1 from t$cnt$idx order by id;
      eval delete from  t$cnt$idx;
    }
    inc $idx;

    if ($is_date == 1)
    {
      let $idx=11;
    }
    commit;
  }
  inc $cnt;
}

--enable_warnings

