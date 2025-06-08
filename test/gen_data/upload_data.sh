#! /bin/bash

HDFS=hdfs

######################################
# 配置区
######################################

# 数据规模（单位GB，例如100表示100GB）        	
SCALE=100
# 生成数据在本地的位置
LOCAL_DIR=~/sda_data/tpcds_gen_${SCALE} 
# 生成数据在HDFS中的位置
HDFS_DIR=/data/tpcds_gen_${SCALE}


# 检查HDFS目标目录是否存在，存在则删除并重新创建
HDFS_DIR_EXISTS=$($HDFS dfs -test -d $HDFS_DIR && echo "true" || echo "false")
if [ "$HDFS_DIR_EXISTS" == "true" ]; then
    echo "HDFS directory $HDFS_DIR has existed. Deleting it..."
    $HDFS dfs -rm -r -f "$HDFS_DIR"
    if [ $? -ne 0 ]; then
        echo "Failed to delete HDFS directory $HDFS_DIR."
        exit 1
    fi
fi
$HDFS dfs -mkdir -p $HDFS_DIR


# 遍历本地目录中的所有文件并复制到HDFS
for FILE in "$LOCAL_DIR"/*; do
    if [ -f "$FILE" ] && [[ "$FILE" == *.dat ]]; then # 只处理 .dat 文件
        FILE_BASENAME=$(basename "$FILE" .dat)
        # 去掉最后的 _数字_数字，比如 _1_4
        TABLE_NAME=$(echo "$FILE_BASENAME" | sed -E 's/_[0-9]+_[0-9]+$//')
        $HDFS dfs -mkdir -p "$HDFS_DIR/$TABLE_NAME"

        HDFS_PATH="$HDFS_DIR/$TABLE_NAME/$FILE_BASENAME"
        echo "Copying file $FILE to $HDFS_PATH ..."
        $HDFS dfs -put "$FILE" "$HDFS_PATH"
        
        if [ $? -ne 0 ]; then
            echo "Failed to copy file $FILE to HDFS."
            exit 1
        fi
    fi
done

$HDFS dfs -chmod -R 777 $HDFS_DIR

echo "Files copied successfully."
