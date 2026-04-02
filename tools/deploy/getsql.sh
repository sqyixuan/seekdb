#!/bin/bash

address=jenkins@10.244.4.66
password=jenkins
sql_pwd=/home/jenkins/workspace/workspace/RQG_1.0_test/obrqg-1.0/randgen/collected_sqls
sql_pwd_master=/home/jenkins/workspace/workspace/RQG_master/obrqg-1.0/randgen/collected_sqls
#拷贝sql文件
expect -c "
spawn scp ${address}:${sql_pwd}/$1 mysql_test/t/ 
expect \"password:\"
send \"${password}\r\"
expect eof
"

expect -c "
spawn scp ${address}:${sql_pwd_master}/$1 mysql_test/t/ 
expect \"password:\"
send \"${password}\r\"
expect eof
"
##拷贝bin文件
#rm -f bin/observer; wget "http://11.166.86.153:8877/observer" -O bin/observer -o ./wget.log && chmod +x bin/observer && ./bin/observer -V;
#修改case文件
echo $1
sed -i '1i\\# owner: rongxuan.lc\n\# owner group: SQL3\n\# description: rqg failed case\n--disable_abort_on_error\n--disable_warnings\n' mysql_test/t/$1  
sed -i '$a\--enable_warnings' mysql_test/t/$1  
sed -i "s/TABLE \/\*! IF EXISTS\*\/ table/TABLE IF EXISTS table/g" mysql_test/t/$1

mv mysql_test/t/$1 mysql_test/t/$1.test

