#!/bin/bash
#for taobao abs
# Usage: oceanbase-build.sh <oceanbasepath> <package> <version> <release>
# Usage: oceanbase-build.sh

if [ $# -ne 4 ]
then
    exit 1
else
    CURDIR=$PWD
    TOP_DIR=`pwd`/../../
    RELEASE="$4"

    export BUILD_NUMBER=${4}
fi

echo "[BUILD] args: TOP_DIR=${TOP_DIR} RELEASE=${RELEASE} BUILD_NUMBER=${BUILD_NUMBER}"

CPU_CORES=`grep -c ^processor /proc/cpuinfo`

cd ${TOP_DIR}

# comment dep_create to prevent t-abs from involking it twice.
./build.sh clean
./build.sh                                  \
    deb                                     \
    -DCPACK_DEBIAN_PACKAGE_RELEASE=$RELEASE \
    -DBUILD_NUMBER=$BUILD_NUMBER            \
    -DUSE_LTO_CACHE=ON			    \
    --init                                  \
    --make deb || exit 1

mv build_deb/*.deb build_deb/*.ddeb $CURDIR || exit 2
