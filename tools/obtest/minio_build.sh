##!bin/bash
current_dir=$1
#mkdir -p $current_dir/minio_build
git clone git@gitlab.oceanbase-dev.com:obqa/obqatool/minio.git
cd minio
#sh reboot_minio.sh -d $current_dir/minio_build -P 11211
sh reboot_minio.sh -d $MINIO_PATH -P 11211
