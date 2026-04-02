#!/bin/bash

OCEANBASE_DIR=$1
httpserver=***

if [ -z $OCEANBASE_DIR ]
then
  if [ -d "../../build_debug" ]
  then
    OCEANBASE_DIR="../../build_debug"
  elif [ -d "../../build_release" ]
  then
    OCEANBASE_DIR="../../build_release"
  elif [ -d "../../build_errsim" ]
  then
    OCEANBASE_DIR="../../build_errsim"
  fi
fi

if [ -z $OCEANBASE_DIR ]
then
    echo "WARN: OCEANBASE_DIR is empty, may be error if not in farm env"
else
  OBCDC_DIR=`pwd`/obcdc
  OBCDC_SO="$OCEANBASE_DIR/src/logservice/libobcdc/src/libobcdc.so.4"
  OBCDC_TAILF="$OCEANBASE_DIR/src/logservice/libobcdc/tests/obcdc_tailf"

  mkdir -p $OBCDC_DIR
  [ -f $OBCDC_SO ] && libtool --mode=install cp  $OBCDC_SO $OBCDC_DIR/
  [ -f $OBCDC_TAILF ] && libtool --mode=install cp $OBCDC_TAILF $OBCDC_DIR/
fi

# OCEANBASE_DIR is empty while executing farm test, the [ -f $OBCDC_TAILF ] will
# exit with code 1(not success), and won't go on with following code in the script invoke copy_obcdc.sh
# MUST exit 0 at the end of this script
echo "copy obcdc executable finish with code $?"
echo "copy timezone_info.conf from $OCEANBASE_DIR/../src/logservice/libobcdc/test/timezone_info.conf to `pwd`/etc/"
mkdir -p `pwd`/etc/ &&
cp $OCEANBASE_DIR/../src/logservice/libobcdc/tests/timezone_info.conf `pwd`/etc/
# echo "begin to wget --no-check-certificate mytest10.jar"
# wget --no-check-certificate ${httpserver}/mytest10.jar -O jar/mytest.jar >/dev/null 2>&1
