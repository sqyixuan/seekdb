#owner : biaonian.wcx
#owner group: storage
#desc: check verified partition for storage cache policy
let $verified_count =query_get_value(SELECT count(*) as count FROM tmp_verified_partitions where part_name = '$part_name', count, 1);
if ($verified_count == 1) {
  --echo Partition $part_name verified for $table_name;
}
if ($verified_count != 1) {
  --echo ERROR: Partition $part_name not verified;
}