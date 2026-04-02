#!/bin/bash
ENV_NAME=$1
cd obtool
./obtool obguard delete env_name="$ENV_NAME" 
cd -
rm -rf obtool
> t/compat/obguard_collect/alarm.log
