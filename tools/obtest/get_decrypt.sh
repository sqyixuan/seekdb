#!bin/bash
# 读取文件内容
python3 $HOME/scripts/crypto/decrypt.py --src_file="storage_media.txt" --dest_file="obtest_decrypt.txt"
if [ ! -s "obtest_decrypt.txt" ]; then
   echo "oss or cos or obs bucket not supported"
   exit 1
fi
file="obtest_decrypt.txt"
line_n=$1
line_num=$((line_n+1))
line=$(sed "${line_num}q;d" $file)
if [ -z "$line" ]; then
  echo "解密后找不到第${line_num}行"
  exit 1
else
  obtest_id=$(echo $line | cut -d',' -f1)
  export OBTEST_AK=$obtest_id
  #echo $OBTEST_AK
  obtest_key=$(echo $line | cut -d',' -f 2-)
  export OBTEST_SK=$obtest_key
  #echo $OBTEST_SK
  #echo "第${line_num}行的内容是：$line"
fi
