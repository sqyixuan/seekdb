#/bin/bash
ENV_NAME=$1
echo $ENV_NAME
HOST_IP=`hostname -i`
echo $HOST_IP
PROXY_PORT=2828

wget http://11.166.86.153:8877/pkg/tool/obtool.tar.gz
tar xzf obtool.tar.gz
rm -f obtool.tar.gz
[ -z "$OBTOOL_SERVER" ] && OBTOOL_SERVER=farm2-obguard.farm2:80

# 部署obguard的账号名
[ -z "$OBTOOL_ACCOUNT" ] && OBTOOL_ACCOUNT="xiaoqizhang.zxq"
cd t/compat/obguard_collect/
python3 $HOME/scripts/crypto/decrypt.py --src_file="obguard_dd.txt" --dest_file="obguard_dd_decrypy.txt"
ddurl=`cat obguard_dd_decrypy.txt`
# 钉钉报警机器人
[ -z "$ALARM_URL" ] && ALARM_URL="https://oapi.dingtalk.com/robot/send?access_token=$ddurl"

# 用于一键提bug的工号ID
[ -z "$EMID" ] && EMID=""
cd -
cd obtool
sed -i "s/^server:.*/server: $OBTOOL_SERVER/" conf/ob_tool.yaml
sed -i "s/^account:.*/account: $OBTOOL_ACCOUNT/" conf/ob_tool.yaml
sed -i "s#^alarmurl:.*#alarmurl: $ALARM_URL#" conf/ob_tool.yaml
sed -i "s/^emid:.*/emid: $EMID/" conf/ob_tool.yaml

./obtool obguard deploy env_name="$ENV_NAME" mysql_host="$HOST_IP"  mysql_port="$PROXY_PORT" mysql_user='root@sys#ob1.root;root@sys#ob2.root' ssh_password="" volatile=true
cd -
> t/compat/obguard_collect/alarm.log
