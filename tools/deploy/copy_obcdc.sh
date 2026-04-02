#!/bin/bash

OBCDC_DIR=`pwd`/obcdc

if [ $# -lt 1 ]
then
#  echo "Usage ./copy_obcdc.sh [oceanbase_dev_dir]"
#  echo "Eg; ./copy_obcdc.sh 1 that means copy from build_debug"
#  echo "Eg: ./copy_obcdc.sh 2 that means copy from build_release"

  if [ -d "../../build_debug" ]
  then
    OCEANBASE_DIR="../../build_debug"
  elif [ -d "../../build_release" ]
  then
    OCEANBASE_DIR="../../build_release"
  fi

else
  #echo $1
  ver_flag=0
  if [ $1 -eq 1 ]
  then
    OCEANBASE_DIR="../../../build_debug"
    ver_flag=1
  elif [ $1 -eq 2 ]
  then
    OCEANBASE_DIR="../../../build_release"
    ver_flag=1
  else
    ver_flag=0
    echo "parameter is invalid"
  fi
fi

if [ -z $OCEANBASE_DIR ]
then
  echo "WARN: OCEANBASE_DIR is empty, may be error if not in farm env"
else
  echo "copy libobcdc.so, obcdc_tailf from "$OCEANBASE_DIR

  OBCDC_SO="$OCEANBASE_DIR/src/logservice/libobcdc/src/libobcdc.so.4"
  OBCDC_TAILF="$OCEANBASE_DIR/src/logservice/libobcdc/tests/obcdc_tailf"

  mkdir -p $OBCDC_DIR
  [ -f $OBCDC_SO ] && libtool --mode=install cp  $OBCDC_SO $OBCDC_DIR/
  [ -f $OBCDC_TAILF ] && libtool --mode=install cp $OBCDC_TAILF $OBCDC_DIR
fi

echo "copy libobcdc and obcdc_tailf finish with code $?"
