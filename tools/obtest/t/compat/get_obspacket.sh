#!/bin/bash
obs_version=$1
obs_packetnum=$2
sh  download_rpm.sh $obs_version $obs_packetnum
wget http://11.166.86.153:8877/obproxy.lts_431 -O $obs_version/bin/obproxy && chmod +x $obs_version/bin/obproxy
