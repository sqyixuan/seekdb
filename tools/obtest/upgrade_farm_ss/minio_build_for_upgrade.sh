##!bin/bash
minio_dir=$1
git clone git@gitlab.oceanbase-dev.com:obqa/obqatool/minio.git
cd minio
sh reboot_minio.sh -d $minio_dir -P 11211
