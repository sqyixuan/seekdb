#/bin/bash
#此脚本为支持小farm ss模式升级

function update_test_config() {
    echo "更新obtest配置文件.."

    cp -rf upgrade_farm_ss/configure_upgrade_temp.ini conf/configure.ini|| exit $?;

    HOST_IP=`hostname -i`
    MINIOURL=`grep "127.0.0.1"  miniourl|awk -F ' ' '{printf $2}'`
    #替换configure.ini配置
    sed -i "s#HOME_PATH#$HOME#g" conf/configure.ini
    escaped_miniourl=$(echo "$MINIOURL" | sed 's/[\/&]/\\&/g')
    sed -i "s|miniourl|$escaped_miniourl|g" conf/configure.ini
    sed -i "s#HOST#$HOST_IP#g" conf/configure.ini
    sed -i "s#devport#$ETHERNET#g" conf/configure.ini 
    sed -i "s#PORT_BASE#$PORT_BASE#g" conf/configure.ini
    sed -i "s#proxy_port#$PROXY_PORT#g" conf/configure.ini
    mkdir -p $HOME/upgrade_data
    mkdir -p $HOME/upgrade_data/workdir
    mkdir -p $HOME/upgrade_data/datadir
}

function build_minio() {
    rm -rf $HOME/minio*
    mkdir -p $HOME/minio_data_dir
    MINIO_PATH=$HOME
    sh -x upgrade_farm_ss/minio_build_for_upgrade.sh $MINIO_PATH >miniourl || exit $?;
}

function update_binary() {

    mkdir -p bin etc
    # 更新observer binary
    echo "从$OBSERVER copy observer binary.."
    cp -rf $OBSERVER bin/observer && chmod +x bin/observer

    # 更新obproxy binary
    echo "从$OBPROXY copy obproxy binary.."
    cp -rf $OBPROXY bin/obproxy && chmod +x bin/obproxy

    # farm 更新 admin
    echo "get admin from $HOME/admin..."
    cp -rf $HOME/admin ./
    
    #farm 更新obcdc
    echo "get obcdc file from $HOME..."
    mkdir obcdc 
    cp -rf $HOME/{libobcdc.so.4,obcdc_tailf} obcdc  && chmod +x obcdc/{libobcdc.so.4,obcdc_tailf}
   
    #farm更换升级起始版本
    echo "try to prepare start version env"
    sh -x copy_for_compat.sh 4_3_5_1,4_3_5_2 || exit $?;
    
}

function collect_log() {
    cp -r collected_log $CASE_COLLECT_LOG_DIR/collected_log
    cp -r log $CASE_COLLECT_LOG_DIR/collected_log/
}

function case_split() {
    cp upgrade_farm_ss/compat_ss.cases testset.list
    sed -i ':a;N;$!ba;s/\n/,/g' testset.list
    sh $HOME/scripts/split_cases.sh
    sed -i ':a;N;$!ba;s/\n/,/g' farm_test.list
}

cd ..
bin/observer -V 2>&1 | tee ./observer.version
bin/obproxy -V 
build_minio
update_binary
update_test_config
case_split
./obtest -t farm_test.list|tee mytestlog_upgrade
if grep -q "FAIL" mytestlog_upgrade; then
     collect_log
     exit 5
fi
