#!/bin/bash

CURRENT_DIR=`pwd`
SHELL_DIR=$CURRENT_DIR/$(dirname $0)
OBCDC_DIR=$CURRENT_DIR/lib
OBCDC_TAILF_DIR=$CURRENT_DIR
PROJECT_ROOT_DIR=$SHELL_DIR/../../../..

if [ $# -lt 1 ]
then
#  echo "Usage ./copy_oblog.sh [oceanbase_dev_dir]"
#  echo "Eg; ./copy_oblog.sh 1 that means copy from build_debug"
#  echo "Eg: ./copy_oblog.sh 2 that means copy from build_release"

  if [ -d "${PROJECT_ROOT_DIR}/build_debug" ]
  then
    OCEANBASE_DIR="${PROJECT_ROOT_DIR}/build_debug"
  elif [ -d "${PROJECT_ROOT_DIR}/build_release" ]
  then
    OCEANBASE_DIR="${PROJECT_ROOT_DIR}/build_release"
  fi

else
  #echo $1
  ver_flag=0
  if [ $1 -eq 1 ]
  then
    OCEANBASE_DIR="{PROJECT_ROOT_DIR}/build_debug"
    ver_flag=1
  elif [ $1 -eq 2 ]
  then
    OCEANBASE_DIR="${PROJECT_ROOT_DIR}/build_release"
    ver_flag=1
  else
    ver_flag=0
    echo "parameter is invalid"
  fi
fi

echo "stopping obcdc..."
$CURRENT_DIR/kill_obcdc.sh

echo "copy libobcdc.so, obcdc_tailf from "$OCEANBASE_DIR

OBCDC_SO="$OCEANBASE_DIR/src/logservice/libobcdc/src/libobcdc.so.4"
OBCDC_TAILF="$OCEANBASE_DIR/src/logservice/libobcdc/tests/obcdc_tailf"

mkdir -p $OBCDC_DIR
[ -f $OBCDC_SO ] && libtool --mode=install cp  $OBCDC_SO $OBCDC_DIR/
[ -f $OBCDC_TAILF ] && libtool --mode=install cp $OBCDC_TAILF $OBCDC_TAILF_DIR

echo "copy timezone_info.conf from $OCEANBASE_DIR/../src/logservice/libobcdc/test/timezone_info.conf to `pwd`/etc/"
mkdir -p `pwd`/etc/ &&
cp $OCEANBASE_DIR/../src/logservice/libobcdc/tests/timezone_info.conf `pwd`/etc/
