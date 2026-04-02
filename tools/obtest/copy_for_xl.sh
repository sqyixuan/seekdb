#!/bin/bash

echo $#
if [ $# -eq 1 ]
then
  START_VERSION=$1
fi

echo $START_VERSION

OCEANBASE_DIR=`pwd`/../../
BIN_DIR=`pwd`/bin${VER}
LIB_DIR=`pwd`/lib
ETC_DIR=`pwd`/etc

if [ $# -lt 2 ]
then
  mkdir -p $BIN_DIR
  mkdir -p $LIB_DIR
  mkdir -p $ETC_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/observer/libobserver.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/liblz4_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/liblzo_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libnone.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libsnappy_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libzlib_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/observer/observer $BIN_DIR/
  wget http://11.166.86.153:8877/obproxy -O $BIN_DIR/obproxy > wget.log 2>&1 && chmod 755 $BIN_DIR/obproxy
fi

rm -rf jar/*.jar
wget "http://11.166.86.153:8877/mytest10.jar" -O jar/mytest.jar -o ./download.log

file_name=$START_VERSION
#wget released upacked rpm history version
echo "now try to wget obs released for $file_name"
logfile=$file_name.wget.log
rm -f $logfile
touch $logfile
rm -rf $file_name
mkdir -p $file_name/bin
mkdir -p $file_name/etc
mkdir -p $file_name/lib
wget -P $file_name/bin --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$file_name/bin/observer 2>&1 && chmod 755 $file_name/bin/observer
wget http://11.166.86.153:8877/obproxy -O $file_name/bin/obproxy --append-output=$logfile 2>&1 && chmod 755 $file_name/bin/obproxy
wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$file_name/etc/upgrade_pre.py 2>&1 && chmod 755 $file_name/etc/upgrade_pre.py
wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$file_name/etc/upgrade_post.py 2>&1 && chmod 755 $file_name/etc/upgrade_post.py
wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/AliTokenizer.conf 2>&1 && chmod 755 $file_name/etc/AliTokenizer.conf
rm -rf $file_name/etc/dict.tar.gz
wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/dict.tar.gz 2>&1 && chmod 755 $file_name/etc/dict.tar.gz
tar -zxvf $file_name/etc/dict.tar.gz -C $file_name/etc/
chmod -R 755 $file_name/etc/dict/

# 下载对应版本的libobcdc
mkdir -p $file_name/obcdc
wget -P $file_name/obcdc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$file_name/obcdc/obcdc_tailf 2>&1 && chmod 755 $file_name/obcdc/obcdc_tailf
wget -P $file_name/obcdc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$file_name/obcdc/libobcdc.so 2>&1 && chmod 755 $file_name/obcdc/libobcdc.so
