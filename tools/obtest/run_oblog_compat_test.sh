#!/bin/bash
#
# 测试obcdc兼容性

#test_cmd_2_2_3="./mytest t/libobcdc/obcdc_compat_backward_2_2_3.test"
#test_cmd_2_2_7="./mytest t/libobcdc/obcdc_compat_backward_2_2_7.test"
test_cmd_4_0_0_0="./mytest t/libobcdc/obcdc_compat_backward_4_0_0_0.test"
test_cmd_4_1_0_0="./mytest t/libobcdc/obcdc_compat_backward_4_1_0_0.test"
test_cmd_current="./mytest t/libobcdc/obcdc_compat_current.test"

function prepare {
  echo "prepare observer bin"
  sh copy_for_compat.sh || exit $?;

  echo "prepare observer bin of current version"
  sh copy.sh -l || exit $?;

  echo "prepare obcdc bin"
  sh copy_obcdc.sh || exit $?;
}

function do_test {

  echo "start to do test cmd is: ${test_cmd_4_0_0_0}"
  (${test_cmd_4_0_0_0}) || return 1;

  echo "start to do test cmd is: ${test_cmd_4_1_0_0}"
  (${test_cmd_4_1_0_0}) || return 1;

  echo "start to do test cmd is: ${test_cmd_current}"
  (${test_cmd_current}) || return 1;

  #echo "start to do test cmd is: ${test_cmd_2_2_3}"
  #(${test_cmd_2_2_3}) || return 1;

  #echo "start to do test cmd is: ${test_cmd_2_2_4}"
  #(${test_cmd_2_2_4}) || return 1;
}

function copy_log {
  [ -d ./collected_log/ ] && mv collected_log/ log/ && echo "mv collected_log to log/"
  [ ! -d ./collected_log/ ] && echo "no collected_log/ directory"
  [ -d ./obcdc/ ] && echo "cp ./obcdc/data.log log/" && cp ./obcdc/data.log log/
  [ ! -d ./obcdc/ ] && echo "no ./obcdc/ directory"
}

echo 'obcdc compat begin'
date

prepare;
do_test;
# 保存返回值
ret_code=$?
copy_log;
# 返回do_test的返回值
exit $ret_code;
