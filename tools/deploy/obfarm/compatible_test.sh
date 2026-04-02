#!/bin/bash
source $CUR_DIR/common.sh
compatible_ip=$(get_one_server)
function init_obtest_config() {
  cd $ROOT_DIR/tools/obtest
  echo -e "1.obs_hosts=$compatible_ip\n" \
          "2.obs_hosts=$compatible_ip\n" \
          "3.obs_hosts=$compatible_ip\n" \
          "proxy_hosts=$compatible_ip\n" \
          "data_path=/home/`whoami`/data10\n" \
          "clog_path=/home/`whoami`/data10\n" \
          "workdir=/home/`whoami`\n" \
          "user=`whoami`\n" \
          "ob_version=1.0\n" \
          "configServer = 10.244.4.23:8080/diamond/cgi/a.py\n" \
          "cleanup=0\n" \
          "save_log=1\n" \
          "save_core=1\n" \
          "init_unit_mem=13487842918\n" \
          "node_cpu_count=16\n" \
          "memory_size_limit=48G" > conf/configure.ini
}

function test_compatible() {
  cd $ROOT_DIR/tools/obtest
  init_obtest_config
  #cp -rf ~/1.1.1_beta .
  #sh copy.sh > /dev/null
  sh copy.sh
  mkdir -p collected_log
  mkdir -p $LOG_DIR/obtest
  #if ! ./mytest t/compat/upgrade_from_1_1_1.test; then
  if ! ./upgrade_test.sh; then
    cp -rf collected_log $LOG_DIR/obtest
    cp -rf bin/observer $LOG_DIR/obtest
    cp -rf bin/obproxy $LOG_DIR/obtest
    cp -rf r/compat/ $LOG_DIR/obtest
    cp -rf my_upgrade_pre.log $LOG_DIR/obtest
    cp -rf my_upgrade_post.log $LOG_DIR/obtest
    mkdir -p $LOG_DIR/obtest/ob1.z1.obs0/log
    mkdir -p $LOG_DIR/obtest/ob1.z2.obs0/log
    cp ~/ob1.z1.obs0/log/* $LOG_DIR/obtest/ob1.z1.obs0/log
    cp ~/ob1.z2.obs0/log/* $LOG_DIR/obtest/ob1.z2.obs0/log
    ./mytest cleanup
    return 2
  fi
  ./mytest cleanup
}
