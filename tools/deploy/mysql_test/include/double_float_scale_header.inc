
let $idx=0;
let $integer_idx=1;
while ($integer_idx <= $integer_cnt)
{
  let $integer_str = query_get_value(select c1 from t_integer where id=$integer_idx, c1, 1);
  let $decimal_idx=1;
  while ($decimal_idx <= $decimal_cnt)
  {
    let $decimal_str = query_get_value(select c1 from t_decimal where id=$decimal_idx, c1, 1);
    let $exp_idx=1;
    while ($exp_idx <= $exp_cnt)
    {
      let $exp_str = query_get_value(select c1 from t_exp where id=$exp_idx, c1, 1);
      --disable_warnings
      --error 0, 1264
      eval insert into $table_name values($idx, '$integer_str.$decimal_str$exp_str', $integer_str.$decimal_str$exp_str);
      --enable_warnings
      inc $exp_idx;
      inc $idx;    
    }
    inc $decimal_idx;
  }
  inc $integer_idx;
}

#let $spec_idx=1;
#while ($spec_idx <= $spec_cnt)
#{
#  let $spec_str = query_get_value(select c1 from t_spec where id=$spec_idx, c1, 1);
#  --disable_warnings
#  --error 0, 1264
#  eval insert into $table_name values($idx, '$spec_str', $spec_str);
#  --enable_warnings
#  inc $spec_idx;
#}



