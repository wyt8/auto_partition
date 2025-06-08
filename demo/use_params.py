from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("iceberg insert using params") \
        .master("yarn") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://master:9083") \
        .enableHiveSupport() \
        .config("auto_partition.use_model", "false") \
        .config("auto_partition.params.init_target_file_size_mb", "16") \
        .config("auto_partition.params.target_file_size_mb", "64") \
        .config("auto_partition.params.max_target_file_size_mb", "512") \
        .config("auto_partition.params.partition_num_knot", "8192") \
        .getOrCreate()
    
    target_database = "target_db"
    target_table = "target_table"
    source_database = "source_db"
    source_table = "source_table"

    spark.sql(f"create database if not exists spark_catalog.{target_database}")

    # 创建表
    spark.sql(
f"""
create table if not exists spark_catalog.{target_database}.{target_table} 
(
id bigint,
name string
)
using iceberg
partitioned by (bucket(0, id))
""")
    
    # 插入数据
    spark.sql(f"insert into spark_catalog.{target_database}.{target_table} select * from spark_catalog.{source_database}.{source_table}")
    
    spark.stop()
