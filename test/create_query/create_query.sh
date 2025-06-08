#! /bin/bash

# 要求：已编译好 dsqgen 工具

######################################
# 配置区
######################################

# 修改为 TPCDS 解压目录
TPCDS_HOME=~/test/DSGen-software-code-3.2.0rc1
# 模板路径
TPCDS_QUERY_TEMPLATE=~/test/create_query/query_templates
# 数据规模（单位GB，例如100表示100GB）
SCALE=100
# 放置输出的查询 SQL 脚本目录
OUTPUT_DIR=~/test/create_query/tpcds_query_${SCALE}
# 随机种子
RNGSEED=1

# 创建输出目录
rm -rf $OUTPUT_DIR
mkdir $OUTPUT_DIR

CUR_DIR=$PWD
cd $TPCDS_HOME/tools
# 运行 dsqgen 生成查询，每个查询生成一个文件
./dsqgen \
    -SCALE $SCALE \
    -OUTPUT_DIR $OUTPUT_DIR \
    -DIRECTORY $TPCDS_QUERY_TEMPLATE \
    -INPUT $TPCDS_QUERY_TEMPLATE/templates.lst \
    -DIALECT presto \
    -RNGSEED $RNGSEED \
    -QUALIFY Y
cd $CUR_DIR

INPUT_FILE=$OUTPUT_DIR/query_0.sql

# 读取输入文件，按 '--' 分割查询
query_counter=1
while IFS= read -r line
do
    # 判断是否遇到查询结束标志（--）
    if [[ "$line" == "--" ]]; then
        # 生成输出文件名
        output_file="$OUTPUT_DIR/query_${query_counter}.sql"
        # 写入查询内容到输出文件
        echo -e "$query_content" > "$output_file"
        # 增加查询计数
        query_counter=$((query_counter + 1))
        # 清空 query_content 以准备下一个查询
        query_content=""
    else
        # 不是 --，再判断是不是非空行
        if [[ -n "$line" ]]; then
            query_content+="$line"$'\n'
        fi
    fi
done < "$INPUT_FILE"

rm $INPUT_FILE

echo "Generated queries are in $OUTPUT_DIR"
