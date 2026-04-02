#!/bin/bash
#export __ScriptPath=`pwd`
#export cfg_file="$__ScriptPath/config.sh"
#source "$cfg_file"
local_ip=`hostname -i`
function init_sysbench_config() {
  echo "data_dir = '~/data'" > $CONFIG_FILE
  echo "workers_per_cpu_quota=4" >> $CONFIG_FILE
  echo "net_thread_count=13" >> $CONFIG_FILE
  echo "oby = OBI(server_spec='$local_ip')" >> $CONFIG_FILE
}

function test_sysbench() {
  cd $DEPLOY_DIR
  sysbench_path="/home/$LOGNAME/sysbench/"
  bg_line_space_pat="'s/^[ \t]*//g'"
  end_line_space_pat='s/[ \t]*$//g'
  if [ -d "$sysbench_path" ]; then
    cp -r ~/sysbench/* .
  fi
  cp -r ~/sysbench/* .
  init_sysbench_config
  ./run_sysbench.pl -c insert -t 64 -p || return 2
  sysbench_insert_info_file="$DEPLOY_DIR/sysbench_insert_64.log"
  avg_insert_info=`grep 'avg:' $sysbench_insert_info_file | sed -e 's/^[ \t]*//g' -e 's/[ \t]*$//g'`
  if [ ! -n "$avg_insert_info" ]; then
    return 2
  fi
  ./run_sysbench.pl -c get -t 64 -p || return 2
  sysbench_info_file="$DEPLOY_DIR/sysbench_get_64.log"
  rw_reguest_info=`grep 'read/write requests' $sysbench_info_file | sed -e 's/^[ \t]*//g' -e 's/[ \t]*$//g'`
  avg_info=`grep 'avg:' $sysbench_info_file | sed -e 's/^[ \t]*//g' -e 's/[ \t]*$//g'`
  approx_info=`grep 'approx.' $sysbench_info_file | sed -e 's/^[ \t]*//g' -e 's/[ \t]*$//g'`
  judge=`echo $approx_info | awk -F ':' '{ print $2 }' | sed -e 's/ms//g' -e 's/^[ \t]*//g' -e 's/[ \t]*$//g'`
  if [ ! -n "$judge" ]; then
    return 2
  fi
  if [ $(echo "$judge>2.5" | bc) -eq 1 ];then
    echo "性能下降!!!"
    return 2
  fi
  echo -e "key:\n    select"  >> $RESULT_FILE
  echo -e "value:\n    $rw_reguest_info\n    $avg_info\n    $approx_info" >> $RESULT_FILE

}

#test_sysbench
