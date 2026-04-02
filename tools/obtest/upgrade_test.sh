#!/bin/bash
#目标点的observer和obproxy的bin文件需要
#提前放到bin目录下面
#

FLAG=$1
echo "FLAG: $FLAG"

test_cmd="./mytest t/compat_farm/upgrade_from_4_3_0_1_lite.test"
test_cmd_ce="./mytest t/compat_farm/upgrade_from_4_X_0_0_CE.test"

function prepare {
  echo "try to prepare test env"
  sh -x copy_for_compat.sh 4_3_3_1,4_3_5_2 || exit $?;

  echo "prepare obcdc test env"
  sh -x copy_obcdc.sh || exit $?;
}

function do_test {
  if [[ "$FLAG" = "ce" ]]; then
   echo "start to do test cmd is: $test_cmd_ce"
   ($test_cmd_ce) || exit 1;
  else
   echo "start to do test cmd is: $test_cmd"
   ($test_cmd) || exit 1;
  fi 
}

prepare;
do_test;
#echo upgrade test will ignore
