let $count=3;
let $retry_block= obdocker.mysql alter system stop zone "z2";
source t/compat/include_for_obtest_ss/retry_obdocker.inc;
