#!/bin/bash
ENV_NAME=$1
touch t/compat/obguard_collect/alarm.log
obtool/obtool obguard allalarm env_name=$ENV_NAME >>t/compat/obguard_collect/alarm.log

if [ "$2" == "upgrade_complete" ];then
    sed -i '/^$/d' t/compat/obguard_collect/alarm.log
    sed -i '/\] \(ERROR\|EDIAG\)/!d' t/compat/obguard_collect/alarm.log
    sed -i '/init io benchmark fail/d' t/compat/obguard_collect/alarm.log
    sed -i '/No such file or directory/d' t/compat/obguard_collect/alarm.log
    sort -u t/compat/obguard_collect/alarm.log >t/compat/obguard_collect/log_error_${ENV_NAME}.log
    err_count=`cat t/compat/obguard_collect/log_error_${ENV_NAME}.log| wc -l`
    echo $err_count
    if [ $err_count -gt 0 ]; then
        cat t/compat/obguard_collect/log_error_${ENV_NAME}.log
    fi
else
   echo "upgrade not complete"
fi
