#!/bin/bash
#目标点的observer和obproxy的bin文件需要
#提前放到bin目录下面
#

test_cmd="./mytest t/storage/leader_migrate.test"

function prepare {
  echo "try to prepare test env"
  sh quick_copy.sh || exit $?;
}

function do_test {
  echo "start to do test cmd is: $test_cmd"
  ($test_cmd) || exit 1;
}

prepare;
do_test;
