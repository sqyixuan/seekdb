#!/bin/bash
cp t/compat/{download_rpm.sh,get_obspacket.sh} ./
sh get_obspacket.sh 4_3_0_0 100010022024022718
sh get_obspacket.sh 4_3_0_1 101010012024041120
sh get_obspacket.sh 4_3_1_0 100040022024070419
sh get_obspacket.sh 4_3_2_0 100010072024072912
sh get_obspacket.sh 4_3_2_1 101010012024091010
sh get_obspacket.sh 4_3_3_0 100000412024101200
sh get_obspacket.sh 4_3_3_1 101070012025021915
sh get_obspacket.sh 4_3_4_0 100010012024112517
sh get_obspacket.sh 4_3_4_1 101010012024121611
sh get_obspacket.sh 4_3_5_0 100070012025040810
sh get_obspacket.sh 4_3_5_1 101050012025042323
sh get_obspacket.sh 4_3_5_2 102000162025051417
wget http://11.166.86.153:8877/pkg/tool/ob_special_config.tar.gz
tar -zxf ob_special_config.tar.gz
sed -i '/ssl_/s/^/#/' ob_special_config/*/sys/config.sql
sed -i '/SSL/s/^/#/' ob_special_config/*/sys/config.sql
wget http://11.166.86.153:8877/pkg/tool/test/mytest.tar.gz
mkdir upgrade_mytest
tar -zxf mytest.tar.gz -C upgrade_mytest
mv -f  upgrade_mytest/mytest/mytest.jar jar
mkdir -p 4_3_2_X/{admin,etc,lib,bin}
mkdir -p 4_3_3_X/{admin,etc,lib,bin}
mkdir -p 4_3_4_X/{admin,etc,lib,bin}
wget http://11.166.86.153:8877/observer.4_3_2_release-7u -O 4_3_2_X/bin/observer 
wget http://11.166.86.153:8877/observer.4_3_3_release-7u -O 4_3_3_X/bin/observer 
wget http://11.166.86.153:8877/observer.4_3_4_release-7u -O 4_3_4_X/bin/observer 
find t/compat -name 'farm*_from_4_*_ss.test' >black_test_upgrade.list
