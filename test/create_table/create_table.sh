#! /bin/bash

HIVE=hive

######################################
# 配置区
######################################

# 数据规模（单位GB，例如100表示100GB）        	
SCALE=1
# 创建外部表的SQL脚本
HIVE_EXTERNAL_TABLES=./hive_external_tables.sql
# 外部表的数据库名
HIVE_EXTERNAL_DB=tpcds_text_${SCALE}
# 生成数据在HDFS中的位置
HDFS_DIR=/data/tpcds_gen_${SCALE}


# 创建外部表
echo "Loading text data into external tables."
$HIVE -f $HIVE_EXTERNAL_TABLES --hivevar DB=$HIVE_EXTERNAL_DB --hivevar LOCATION=$HDFS_DIR




# 事实表和维度表
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"




# # 将外部表转换为Iceberg表
# LOAD_FILE="tpcds_iceberg_3_${SCALE}.mk"
# echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

# i=1
# total=24
# DATABASE=tpcds_iceberg_3_${SCALE}

# for t in ${DIMS}
# do
# 	COMMAND="$SPARK --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1 \\
# 		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
# 		--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \\
# 		--conf spark.sql.catalog.spark_catalog.type=hive \\
# 		--conf spark.sql.catalog.spark_catalog.uri=thrift://master:9083 \\
# 		--executor-memory 4g \\
# 		-v -f ddl-tpcds/bin_partitioned_2/${t}.sql \\
# 		-d SPARK_CATALOG=spark_catalog -d DB=${DATABASE} -d SOURCE=tpcds_text_1 -d SCALE=1 -d PARTITION_NUM=10"
# 	echo -e "${t}:\n\t@$COMMAND && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
# 	i=`expr $i + 1`
# done

# for t in ${FACTS}
# do
# 	COMMAND="$SPARK --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1 \\
# 		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
# 		--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \\
# 		--conf spark.sql.catalog.spark_catalog.type=hive \\
# 		--conf spark.sql.catalog.spark_catalog.uri=thrift://master:9083 \\
# 		--executor-memory 4g \\
# 		-v -f ddl-tpcds/bin_partitioned_2/${t}.sql \\
# 		-d SPARK_CATALOG=spark_catalog -d DB=${DATABASE} -d SOURCE=tpcds_text_1 -d SCALE=1 -d PARTITION_NUM=10"
# 	echo -e "${t}:\n\t@$COMMAND && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
# 	i=`expr $i + 1`
# done

# make -j 1 -f $LOAD_FILE



# # # 添加约束
# # echo "Adding constraints"
# # $HIVE -f ddl-tpcds/bin_partitioned/add_constraints.sql --hivevar DB=${DATABASE}

# echo "All have finished!"