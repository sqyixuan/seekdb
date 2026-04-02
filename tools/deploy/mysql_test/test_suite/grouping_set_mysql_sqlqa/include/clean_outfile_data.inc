let $HOST_IP = query_get_value('select host_ip()',host_ip(), 1);
let $RM_CMD = ssh $HOST_IP 'rm -rf "$OBSERVER_DIR/TEST_GROUPING"' ;

exec $RM_CMD;