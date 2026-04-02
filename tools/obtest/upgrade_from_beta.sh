#!/bin/bash

echo -e "1.obs_hosts=$1\n" \
        "2.obs_hosts=$1\n" \
        "3.obs_hosts=$1\n" \
        "proxy_hosts=$1\n" \
        "data_path=/home/tingting.rtt/data10\n" \
        "clog_path=/home/tingting.rtt/data10\n" \
        "workdir=/home/tingting.rtt\n" \
        "user=tingting.rtt\n" \
        "ob_version=1.0\n" \
        "configServer = 10.244.4.23:8080/diamond/cgi/a.py\n" \
        "cleanup=0\n" \
        "save_log=1\n" \
        "save_core=0\n" \
        "init_unit_mem=13487842918\n" \
        "node_cpu_count=16\n" \
        "memory_size_limit=48G" > conf/configure.ini

#cp -rf ~/1.0.3_beta .
#sh copy.sh
#mkdir -p collected_log
#./mytest t/compat/upgrade_from_beta3.test || echo "FAILED UPGRADE!"
#./mytest cleanup

