#!/bin/bash

# $1: case name

if [ $# < 1 ]; then
  echo "you need a case_name at lease";
fi

if [ $# == 2 ]; then
./deploy.py obrqg.reboot
fi

./deploy.py obrqg.mysqltest testset=$1 disable-reboot mysql=127.0.0.1:3308
