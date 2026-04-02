#!/bin/bash
cp t/compat/{download_rpm.sh,get_obspacket.sh} ./
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
find t/compat -name 'farm*_from_4_*_ss.test' >black_test_upgrade.list
cd t/compat
python3 $HOME/scripts/crypto/decrypt.py --src_file="cdc_bin_en.txt" --dest_file="cdc_bin.txt"
python3 $HOME/scripts/crypto/decrypt.py --src_file="cdc_tailf_en.txt" --dest_file="cdc_tailf.txt"
cdcbinurl=`cat cdc_bin.txt`
cdctailfurl=`cat cdc_tailf.txt`
cd -
mkdir obcdc
wget $cdcbinurl -O obcdc/libobcdc.so.4
wget $cdctailfurl -O obcdc/obcdc_tailf
chmod +x obcdc/libobcdc.so.4 obcdc/obcdc_tailf
