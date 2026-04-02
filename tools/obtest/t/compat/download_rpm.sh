#!/bin/bash

function usage(){
    echo -e "usage: \n    sh ${0} [obversion] [aoneid]"
    echo -e "    e.g. \n        sh ${0} 1_4_74, automatically find latest aone id and update"
    echo -e "     or \n        sh ${0} 1_4_74 1765224, update use the given version and aone id"
    echo -e "     or \n        sh ${0}, fully automatically update"
}

function get_newest_rpm(){
    echo "try to find newest rpm of $version"
    dot_version=`echo ${version} | sed -e 's/_/./g'`
    url_dir="http://yum.tbsite.net/taobao/7/x86_64/current/oceanbase/"
    #e.g. oceanbase-2.1.0-1749975.el6.x86_64.rpm -> oceanbase-${version}-${aoneno}.el6.x86_64.rpm
    if [ -n "$aoneid" ]; then
        name="oceanbase-${dot_version}-${aoneid}.el7.x86_64.rpm"
    else
        name=`curl -s $url_dir  | grep -oE ">oceanbase.*\-${dot_version}\-.*rpm"|grep -oE "oceanbase.*\-${dot_version}\-.*rpm"|sort|tail -1`
    fi
    echo "find newest rpm $name"
}

function update_according_to_version(){
    echo "Rise 8877 $version release"
    if [[ $is_update_bin == true ]];then
        folder="${version}_dst_rpm"
    else
        folder=$version
    fi
    dot_version=`echo ${version} | sed -e 's/_/./g'`
    if [ ! -d $folder ]; then
        echo "$folder not exist, mkdir "
        mkdir $folder
    fi
    
    cd $folder
    echo "change work directory to: `pwd`"
    
    url_dir="http://yum.tbsite.net/taobao/7/x86_64/current/oceanbase/"
    url_dir_test="http://yum.tbsite.net/taobao/7/x86_64/test/oceanbase/"
    #e.g. oceanbase-2.1.0-1749975.el6.x86_64.rpm -> oceanbase-${version}-${aoneno}.el6.x86_64.rpm
    if [ -n "$aoneid" ]; then
        name="oceanbase-${dot_version}-${aoneid}.el7.x86_64.rpm"
    else
        echo "start to found the latest rpm ..."
        sleep 1
        name=`curl -s $url_dir  | grep -oE ">oceanbase.*\-${dot_version}\-.*rpm"|grep -oE "oceanbase.*\-${dot_version}\-.*rpm"|sort|tail -1`
        #if [[ $list_update != 1 ]]; then
        #    echo "please confirm if $name is correct?"
        #    if ( whiptail --title "Is this rpm correct?" --yesno "$name" 10 60 ) then
        #        echo "you choose yes, rpm is: $name"
        #        #exit 0
        #    else
        #        echo "you choose no, please check what happened at: $url_dir"
        #        return 0
        #    fi
        #else
        #    echo "automatically judge rpm is: $name"
        #fi
        echo "automatically judge rpm is: $name"
    fi
    
    full_url=$url_dir$name
    echo "start to wget from current: $full_url"
    
    if [ -f "$name" ]; then
        echo "$name exists, delete it first, then download a new one"
        rm -rf $name
    fi
    wget $full_url -o ${version}-${aoneid}.wget.log
    wget_ret=$?
    if [[ $wget_ret -ne 0 ]];then
        full_url=$url_dir_test$name
        echo "wget from current failed, start to wget from test: $full_url"
        wget $full_url -o ${version}-${aoneid}.wget.log
        wget_ret=$?
        if [[ $wget_ret -ne 0 ]];then 
            echo "get from test failed too, 请检查rpm库里面${name}是否存在，如果没有可以尝试从回收站移回"
            exit $wget_ret
        fi
    fi
    sleep 1
    
    if [ -d "./home" ]; then
        echo "rm -rf ./home"
        rm -rf ./home
    fi
    
    echo "extract rpm ..."
    rpm2cpio $name | cpio -idmv 
    
    if [ ! -d "./bin" ]; then
        echo "bin not exists, mkdir"
        mkdir bin
    fi
    if [ ! -d "./lib" ]; then
        echo "lib not exists, mkdir"
        mkdir lib
    fi 
   
    echo "copy observer etc lib ..."
    cp -fp home/admin/oceanbase/bin/observer ./bin/ && chmod 777 ./bin/observer
    cp -rfp home/admin/oceanbase/etc ./ && chmod +x ./etc/*.py
    if [[ -d "home/admin/oceanbase/lib" ]];then
        cp -rfp home/admin/oceanbase/lib ./
    fi
    cp -rfp home/admin/oceanbase/admin ./
    
    echo "rm -rf ./home $name"
    rm -rf ./home $name
    
    version_name=$version_name""$version": "$name"  \\n "
    echo $name > current_version.txt
    echo "change work directory back"
    cd -
    echo "done."
}

function update_according_to_list(){
    #cat observer.list|while read line
    while read line
    do
        version=$line
        if [ ! -d $version ]; then
            echo "$version does not exist, try to download ... "
            update_according_to_version
        else
            if [[ -e $version/current_version.txt ]]; then
                current_rpm=`cat ${version}/current_version.txt`
            elif [[ -e $version/bin/observer ]]; then 
                aone_id_str=`$version/bin/observer -V 2>&1 | grep observer | grep -oE "[0-9]{7}\.el7"`
                dot_version=`echo ${version} | sed -e 's/_/./g'`
                current_rpm="oceanbase-${dot_version}-${aone_id_str}.x86_64.rpm"
            else 
                echo "$version does not exist, try to download ... "
                update_according_to_version
                continue
            fi
            echo "$version exists, check if $current_rpm is the newest"
            get_newest_rpm
            if [[ "$name" > $current_rpm ]]; then
                echo "$current_rpm is not newest, newest is $name, rise it"
                update_according_to_version 
            else
                echo "$current_rpm is the newest, skip $version"
            fi
        fi
    done < observer.list
}

function build_observer_list(){
    echo "build observer.list"
    aone_repo_vers=`curl -s "http://yum.tbsite.net/taobao/7/x86_64/current/oceanbase/"|grep -oE ">oceanbase.*.rpm"|grep -oE "oceanbase.*.rpm"|grep -oE "\-[1-9]{1}\.[0-9]{1}\.[0-9]{1,2}\-"|grep -oE "[1-9]{1}\.[0-9]{1}\.[0-9]{1,2}"|uniq|sed -e "s/\./_/g"`
    echo "observer.list:"
    echo "$aone_repo_vers" | tee observer.list
}


#Entrance
if [ "X"$1 == "X-h" -o "X"$1 == "X--help" ]; then
    usage
    exit 1
fi

#osversion=6 #在外面设置,默认6
if [[ "X"$osversion == "X" ]];then
    osversion=7
fi
version=
aoneid=
list_update=0
version_name=
name=
work_dir=`pwd`
cd $work_dir

echo " "
echo "=========================================================="
echo "=======>`date '+%Y-%m-%d %H:%M:%S'` rise_8877_rpm started."

case $# in
    0)
        echo "Rise 8877 rpm fully automatically"
        build_observer_list
        list_update=1
        update_according_to_list
        ;;
    1|2|3)
        version=$1
        aoneid=$2
        is_update_bin=$3
        update_according_to_version
        ;;
    *)
        usage
        exit 1
esac




