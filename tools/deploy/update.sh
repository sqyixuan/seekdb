#!/bin/bash

httpserver=http://11.166.86.153:8877

# download mytest.jar
if [ ! -f mysql_test/java/mytest.jar ]; then
  echo "begin to wget mytest10.jar ..."
  wget ${httpserver}/mytest10.jar -O mysql_test/java/mytest.jar >/dev/null 2>&1
fi

VERSION=.master

# download bin (default)
if [ ! -n "$1" ];then
  echo "begin to wget observer/obproxy ..."
  mkdir -p bin
  wget ${httpserver}/observer$VERSION -O bin/observer
  wget ${httpserver}/obproxy.support_4.0 -O bin/obproxy
  chmod +x bin/*

  # download libobcdc
  echo "begin to wget libobcdc ..."
  mkdir -p obcdc
  wget ${httpserver}/obcdc_tailf$VERSION -O obcdc/obcdc_tailf
  wget ${httpserver}/libobcdc.so.1$VERSION -O obcdc/libobcdc.so.1
  wget ${httpserver}/libdrcmsg.so -O obcdc/libdrcmsg.so
  chmod +x obcdc/obcdc_tailf
  chmod +x obcdc/libobcdc.so.1
  chmod +x obcdc/libdrcmsg.so
fi
