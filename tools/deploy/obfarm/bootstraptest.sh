#!/bin/bash

source $CUR_DIR/common.sh
bootstrap_ip=$(get_one_server)
result="$CUR_DIR/bs_result"
function init_boostrap_config() {
  echo "app_name = 'bst'" > $CONFIG_FILE
  echo "data_dir = '~/data'" >> $CONFIG_FILE
  echo "bst = OBI(server_spec='[$bootstrap_ip,proxy@$bootstrap_ip]@zone1 [$bootstrap_ip]@zone2')" >> $CONFIG_FILE
}
function boostrap_log() {
  mkdir -p $LOG_DIR/boostrap
  cd /home/$LOGNAME
  #if find ob1* -name core*; then
  #  echo "bootstrap have core"
  #fi
  cp -rf bst* $LOG_DIR/boostrap
  rm -rf bst*
}
function test_boostrap(){
  cd $DEPLOY_DIR
  ./copy.sh > /dev/null 2>&1
  init_boostrap_config
  python deploy.py bst.force_stop > /dev/null
  python deploy.py bst.reboot | tee $result
  python deploy.py bst.force_stop > /dev/null
  if egrep -q "bootstrap\s*timeout|Fail:|ERROR|Errno 2" $result; then
    boostrap_log
    return 2;
  fi
}
