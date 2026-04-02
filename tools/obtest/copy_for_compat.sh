#!/bin/bash

echo $#
if [ $# -eq 1 ]
then
  echo "yes"
  DOWNLOAD_VERSION=$1
fi

echo $DOWNLOAD_VERSION

OCEANBASE_DIR=`pwd`/../../
BIN_DIR=`pwd`/bin${VER}
LIB_DIR=`pwd`/lib
ETC_DIR=`pwd`/etc
ADMIN_DIR=`pwd`/admin
CE_DIR=`pwd`/4_X_0_0_CE

if [ $# -lt 2 ]
then
  mkdir -p $BIN_DIR
  mkdir -p $LIB_DIR
  mkdir -p $ETC_DIR
  mkdir -p $ADMIN_DIR
  mkdir -p $CE_DIR/bin
  mkdir -p $CE_DIR/lib
  mkdir -p $CE_DIR/etc
  mkdir -p $CE_DIR/admin
  libtool --mode=install cp $OCEANBASE_DIR/src/observer/libobserver.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/liblz4_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/liblzo_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libnone.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libsnappy_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/lib/compress/libzlib_1.0.la $LIB_DIR
  libtool --mode=install cp $OCEANBASE_DIR/src/observer/observer $BIN_DIR/
  #wget http://11.166.86.153:8877/obproxy -O $BIN_DIR/obproxy > wget.log 2>&1 && chmod 755 $BIN_DIR/obproxy
  libtool --mode=install cp $OCEANBASE_DIR/src/share/inner_table/sys_package/*.sql $ADMIN_DIR
  wget http://11.166.86.153:8877/mytest_latest.jar -O jar/mytest.jar > wget.log 2>&1 &
#  if [ -x ../obproxy/obproxy ];then
#	  cp ../obproxy/obproxy $BIN_DIR/obproxy
#  else
#	  cp $HOME/obproxy $BIN_DIR/obproxy
#  fi
fi

function prepareObserverByVerison {
  file_name=$1
  remote_file_name=$2
  # do whatever on $i
  echo "now try to wget obs released for $file_name, $remote_file_name"
  logfile=$file_name.wget.log
  rm -f $logfile
  touch $logfile
  if [[ -d $file_name ]]
  then
     if [[ $WITHOUT_OVERWRITE ]]
     then
       echo "skip cp $file_name"
       return
     else
       rm -rf $file_name
     fi
  fi
  mkdir -p $file_name/bin
  mkdir -p $file_name/etc
  mkdir -p $file_name/lib
  wget -P $file_name/bin --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$remote_file_name/bin/observer 2>&1 && chmod 755 $file_name/bin/observer

  wget http://11.166.86.153:8877/obproxy.support_4.0 -O $file_name/bin/obproxy --append-output=$logfile 2>&1 && chmod 755 $file_name/bin/obproxy
  wget http://11.166.86.153:8877/obproxy-ce-4.1.0.0_dev -O $CE_DIR/bin/obproxy --append-output=4_X_0_0_CE.log 2>&1 && chmod 755 $CE_DIR/bin/obproxy
  #if [ -x ../obproxy/obproxy ];then
  #  cp ../obproxy/obproxy $file_name/bin/obproxy
  #else
  #  cp $HOME/obproxy $file_name/bin/obproxy
  #fi

  wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$remote_file_name/etc/upgrade_pre.py 2>&1 && chmod 755 $file_name/etc/upgrade_pre.py
  wget -P $file_name/etc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$remote_file_name/etc/upgrade_post.py 2>&1 && chmod 755 $file_name/etc/upgrade_post.py
  wget -P $file_name -r -np -nH --cut-dirs=2 http://11.166.86.153:8877/oceanbase_released/$remote_file_name/admin/


  # 下载对应版本的libobcdc
  mkdir -p $file_name/obcdc
  wget -P $file_name/obcdc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$remote_file_name/obcdc/obcdc_tailf 2>&1 && chmod 755 $file_name/obcdc/obcdc_tailf
  wget -P $file_name/obcdc --append-output=$logfile http://11.166.86.153:8877/oceanbase_released/$remote_file_name/obcdc/libobcdc.so 2>&1 && chmod 755 $file_name/obcdc/libobcdc.so
}

#wget released upacked rpm history version
pids=()
IFS=',' read -ra file_names <<< "$DOWNLOAD_VERSION"
if [ ${#file_names[@]} -eq 0 ]; then
    mapfile -t file_names < observer.list
fi
echo "All file names: ${file_names[@]}"
for file_name in "${file_names[@]}"; do
    echo $file_name
    remote_file_name=$file_name
   # 这段不要删，下次不需要的时候注释就可以了
   # 替换observer的方法见
   if [[ "$file_name" = "2_2_60" ]]; then
     remote_file_name="2_2_60_20200818"
   fi

   if [[ "$file_name" = "3_1_0" ]]; then
     remote_file_name="3_1_0_20210412"
   fi

   prepareObserverByVerison $file_name $remote_file_name &
   pids+=($!)
done

for pid in "${pids[@]}"; 
do
    wait $pid
done
