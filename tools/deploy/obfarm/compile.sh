#!/bin/bash


function Compile() {
  cd $ROOT_DIR/rpm
  source /etc/profile
  dep_create oceanbase
  cd $ROOT_DIR
  ./build.sh init || return 2
  ./build.sh || return 2
  make clean
  ./configure --with-release --without-test-case --enable-buildtime=yes --enable-shared=default --enable-silent-rules || return 2
  cd $ROOT_DIR/src
  make -j 20 || return 2
  echo "finish compile src"
}
