# sh pl_warp_input.sh
# 加密 input_file 文件夹下用例到 output_file

current_path=$(pwd)
suites='pl_wrap_pl_sqlqa'

for casename in `ls -lrt ${current_path}/input_file/|grep sql|awk -F' ' '{print $NF}' |awk -F'.' '{print $1}'`;do
  echo "casename: $casename"
  ../wrap ${current_path}/input_file/${casename}.sql  -o ${current_path}/output_file/${casename}.sql -c
done

