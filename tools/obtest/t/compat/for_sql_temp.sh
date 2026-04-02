#!/bin/bash
while IFS=' ' read -ra line_data; do
    start_version_for_report="${line_data[0]}"
    target_branch_for_report="$1"
    target_version_for_report="$2"
    target_commit_for_report="${line_data[1]}"
    upgrade_path_for_report="${line_data[0]}\ ->\ $target_branch_for_report"
    echo ${line_data[7]};
    if [ "${line_data[7]}" != "nobarrier" ];then
        upgrade_path_for_report="${line_data[0]}\ ->\ "
        index=7  # 开始于第8个元素的索引
        echo ${line_data[7]}+"ssss";
        while [ -n "${line_data[index]}" ]; do
            upgrade_path_for_report+="${line_data[index]}\ ->\ "
            ((index++))
        done
       upgrade_path_for_report+="$target_branch_for_report"
    fi
    is_standby=$3
    is_asvr=$4
    proxy_version_for_report="${line_data[2]}"
    start_time="${line_data[3]}\ ${line_data[4]}"
    end_time="${line_data[5]}\ ${line_data[6]}"
    echo $start_version_for_report
    echo $target_branch_for_report
    echo $target_version_for_report
    echo $target_commit_for_report
    echo $upgrade_path_for_report
    echo $is_standby
    echo $is_asvr
    echo $proxy_version_for_report
    echo $start_time
    echo $end_time
    obclient -h 100.88.105.219 -P2121 -uroot@tt_ww_mysql#ob_rise.winwin.100.88.105.219 -Dupgrade_test  -e "insert into upgrade_result_report_for_obtest(start_version,target_branch,target_version,target_commit,upgrade_path,is_standby,is_asvr,proxy_version,start_time,end_time) values ('$start_version_for_report','$target_branch_for_report','$target_version_for_report','$target_commit_for_report','$upgrade_path_for_report',$is_standby,$is_asvr,'$proxy_version_for_report', '$start_time', '$end_time') ON DUPLICATE KEY UPDATE target_branch='$target_branch_for_report', target_commit='$target_commit_for_report',upgrade_path='$upgrade_path_for_report',proxy_version='$proxy_version_for_report', start_time='$start_time',end_time='$end_time';"
    #create table upgrade_result_report_for_obtest(id int not null auto_increment, start_version varchar(100) not null, target_branch varchar(100) not null, target_version varchar(100) not null, target_commit varchar(100) not null, upgrade_path varchar(100) not null, is_standby int not null, proxy_version varchar(100) not null, start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP , end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, primary key(start_version,target_version,is_standby));
done < for_mysql.txt
