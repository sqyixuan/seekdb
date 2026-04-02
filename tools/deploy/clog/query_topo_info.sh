#!/bin/bash

if [ $# != 5 ]; then
echo "---------------------------"
echo "Usage: ./topo-tree.py  svr_ip  svr_port  table_id  partition_idx  partition_cnt"
echo "---------------------------"
exit 1;
fi

HOST="10.244.4.27"
PORT="29613"
USER="root@sys"
DB="oceanbase"

TID="1100611139453777"
P_IDX="0"
P_CNT="1"

HOST=$1
PORT=$2
TID=$3
P_IDX=$4
P_CNT=$5

#mysql -h $HOST -P$PORT -u$USER -D $DB  < query_topo_info.sql  > /tmp/result-topo.txt

#mysql -h $HOST -P$PORT -u$USER -D $DB -e "select svr_ip, svr_port, table_id, partition_idx, partition_cnt, role, parent, children_list from __all_virtual_clog_stat where table_id = $TID and partition_idx = $P_IDX and partition_cnt = $P_CNT;"  | tee -a /tmp/topo-query-result.txt

mysql -h $HOST -P$PORT -u$USER -D $DB -e "select svr_ip, svr_port, table_id, partition_idx, partition_cnt, role, parent, children_list from __all_virtual_clog_stat where table_id = $TID and partition_idx = $P_IDX and partition_cnt = $P_CNT;"  > /tmp/topo-query-result.txt
