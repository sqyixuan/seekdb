# owner: jianyun.sjy
# owner group: storage
# description: 同zone内快速迁移的脚本，需要提供$table_id
# tags: fast_migrate


# prepare
connect (fast_mig_obsys,$OBMYSQL_MS0,root,,oceanbase,$OBMYSQL_PORT);
if ($role == "") {
  let $role = 1;
}
if ($timeout == "") {
  let $timeout = 30;
}
--echo role = $role
--echo timeout = $timeout

# get src_ip, src_port, dest_ip, dest_port
connection fast_mig_obsys;

let $zone = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and role = $role, zone, 1);

let $src_ip = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and zone = "$zone", svr_ip, 1);

let $src_port = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and zone = "$zone" and svr_ip = "$src_ip", svr_port, 1);

let $dest_ip = query_get_value(select * from __all_server where zone = "$zone" and (svr_ip != "$src_ip" or svr_port != $src_port), svr_ip, 1);

let $dest_port = query_get_value(select * from __all_server where zone = "$zone" and (svr_ip != "$src_ip" or svr_port != $src_port) and svr_ip = "$dest_ip", svr_port, 1);

# trigger partition move
--disable_query_log
eval alter system move replica partition_id = '0%1@$table_id' source='$src_ip:$src_port' destination='$dest_ip:$dest_port';

# verify
let $__i__ = 0;
while ($__i__ < $timeout) {
  let $now_ip = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and zone = "$zone", svr_ip, 1);

  let $now_port = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and zone = "$zone" and svr_ip = "$now_ip", svr_port, 1);

  let $status = query_get_value(select * from __all_virtual_meta_table where table_id = $table_id and zone = "$zone" and svr_ip = "$now_ip", status, 1);

#  --echo table_id: $table_id, src: $src_ip:$src_port, dest: $dest_ip:$dest_port, now: $now_ip:$now_port, status: $status
  if ($now_ip == $dest_ip) {
    if ($now_port == $dest_port) {
      if ($status == "NORMAL") {
        --echo fast migrate success
        let $__i__ = $timeout;
      }
    }
  }

  sleep 1;
  inc $__i__;
}
--enable_query_log

# clean up
disconnect fast_mig_obsys; 
