#!/bin/bash

if [ $# -lt 1 ]; then
  OCEANBASE_DIR=`pwd`/../../
else
  OCEANBASE_DIR=$1
fi

BIN_DIR=`pwd`/bin${VER}
if [ $# -lt 2 ]; then
  cd $OCEANBASE_DIR/tools/trace;
  make clean;
  make;
  if file trace_tool | grep -q "POSIX shell script text executable"; then
    set -x
    cp $OCEANBASE_DIR/tools/trace/.libs/trace_tool $BIN_DIR/
  else
    set -x
    cp $OCEANBASE_DIR/tools/trace/trace_tool $BIN_DIR/
  fi
fi
