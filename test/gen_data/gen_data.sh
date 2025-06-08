#!/bin/bash

# TPC-DS 数据生成脚本
# 要求：已编译好 dsdgen 工具

######################################
# 配置区
######################################

# 修改为 TPCDS 解压目录
TPCDS_HOME=~/test/DSGen-software-code-3.2.0rc1
# 数据规模（单位GB，例如100表示100GB）        	
SCALE=100
# 并行进程数量                              
PARALLELISM=16
# 修改为保存生成数据的位置
LOCAL_DIR=~/sda_data/tpcds_gen_${SCALE} 


# 打印配置信息
echo "开始生成 TPC-DS 数据"
echo "规模: ${SCALE}GB"
echo "并行度: $PARALLELISM"
echo "输出目录: $LOCAL_DIR"


# 创建输出目录
rm -rf $LOCAL_DIR
mkdir $LOCAL_DIR

# 生成数据（并行启动多个子进程）
for ((i=1; i<=PARALLELISM; i++)); do
  echo "启动子任务 $i/$PARALLELISM..."
  CUR_DIR=$PWD
  cd $TPCDS_HOME/tools
  ./dsdgen -scale $SCALE -dir $LOCAL_DIR -PARALLEL $PARALLELISM -CHILD $i > "$LOCAL_DIR/dsdgen_child_$i.log" 2>&1 &
  cd $CUR_DIR
done

# 等待所有子进程完成
wait

echo "所有数据生成任务完成！"
echo "数据已生成在: $LOCAL_DIR"

# 统计输出目录下的文件数量和大小
echo "生成文件统计："
ls -lh $LOCAL_DIR
