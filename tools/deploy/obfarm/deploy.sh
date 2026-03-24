#!/bin/bash

export LOG_DIR=~/logs/$1
export __ScriptPath=`pwd`
export cfg_file="$__ScriptPath/config.sh"
export compile_file="$__ScriptPath/compile.sh"
export smalltest_file="$__ScriptPath/smalltest.sh"
export bootstrap_file="$__ScriptPath/bootstraptest.sh"
export sysbench_file="$__ScriptPath/sysbenchtest.sh"
export compatible_file="$__ScriptPath/compatible_test.sh"
mkdir -p $LOG_DIR
function curTime() {
  CURTIME=`date +%Y-%m-%d-%H:%M:%S`
  echo "==========$CURTIME=========="
}
function sourceFile() {
  if ! source "$1"; then
    echo "!!! FATAL: failed to source $1"
    exit 2
  fi
}
sourceFile $cfg_file
sourceFile $compile_file
sourceFile $smalltest_file
sourceFile $bootstrap_file
sourceFile $sysbench_file
sourceFile $compatible_file
#编译
if [ ! $2 -o $2 = "compile" ]; then
curTime
if ! Compile ; then
  echo "===========Compile Fail==========="
  exit 3
fi
fi

#test bootstrap
if [ ! $2 -o $2 = "bootstrap" ]; then
curTime
if ! test_boostrap; then
  echo "===========booststrap failed============"
  exit 4
fi
fi

## There's no need to run psmalltest in obfarm, condor will take over
## it.
# #psmalltest
# if [ ! $2 -o $2 = "psmalltest" ]; then
# curTime
# if ! test_smalltest ; then
#   echo "===========psmalltest failed==========="
#   exit 5
# fi
# curTime
# fi

#兼容性测试
if [ ! $2 -o $2 = "compatible" ]; then
curTime
if ! test_compatible; then
  echo "===========兼容性测试失败=========="
  exit 6
fi
fi

#sysbench
if [ ! $2 -o $2 = "sysbench" ]; then
curTime
if ! test_sysbench; then
  echo "===========sysbench failed============="
  exit 7
fi
curTime
fi
