#!/bin/bash
arg_count=$#
obstartv=$2
barrierv=$3
$obstartv/bin/observer -V 2>&1 | tee $obstartv/bin/observer.version;
obstartv_bak=`echo "$obstartv" | sed 's/_/./g'`
echo "$obstartv_bak" > for_mysql.txt;
start_version=`cat $obstartv/bin/observer.version|grep REVISION|awk -F " " '{print $2}'|awk -F "-" '{print $1}'`;
start_version="-$start_version"
if [[ $obstartv == *"X"* ]]; then
   #start_version=`cat $obstartv/bin/observer.version|grep REVISION|awk -F "-" '{print $2}'`
   start_version=""
elif [[ $obstartv == 全路径* ]]; then
   obstartv=`echo $obstartv|awk -F "-" '{print $2}'`
   $obstartv/bin/observer -V 2>&1 | tee $obstartv/bin/observer.version
   echo $obstartv
   start_version=`cat $obstartv/bin/observer.version|grep REVISION|awk -F " " '{print $2}'|awk -F "-" '{print $1}'`
   start_version="-$start_version"
   echo $start_version
fi
sed -i "$ s/$/$start_version /" for_mysql.txt;

bin/observer -V 2>&1 | tee bin/targetobserver.version;
target_version=`cat bin/targetobserver.version|grep "REVISION"|awk -F "-" '{print $2}'`;
sed -i "$ s/$/$target_version /" for_mysql.txt;

$obstartv/bin/obproxy -V 2>&1 | tee $obstartv/bin/obproxy.version;
obproxy_version=`cat $obstartv/bin/obproxy.version |grep obproxy|grep OceanBase|awk -F " " '{print $3}'`;
obproxy_commit=`cat $obstartv/bin/obproxy.version |grep REVISION|awk  -F "-" '{print $3}'`;
sed -i "$ s/$/$obproxy_version-/" for_mysql.txt;
sed -i "$ s/$/$obproxy_commit /" for_mysql.txt;

startdata="$1"
echo $startdata
sed -i "$ s/$/$startdata /" for_mysql.txt;

enddate=`date +"%Y-%m-%d %H:%M:%S"`
echo $enddate
sed -i "$ s/$/$enddate /" for_mysql.txt;

if [[ $# -gt 2 ]]; then
   for ((i=3; i<=$#; i++)); do
       ${!i}/bin/observer -V 2>&1 | tee ${!i}/bin/observer.version;
       barrier_version=`cat ${!i}/bin/observer.version|grep REVISION|awk -F " " '{print $2}'|awk -F "-" '{print $1}'` 
       obbarrierv_bak=`echo "${!i}" | sed 's/_/./g'`
       sed -i "$ s/$/$obbarrierv_bak-/" for_mysql.txt;
       sed -i "$ s/$/$barrier_version /" for_mysql.txt;
   done
else 
   sed -i "$ s/$/nobarrier /" for_mysql.txt;
fi
