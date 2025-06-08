import os
from pyspark.sql import SparkSession

def create_table(
        spark: SparkSession,
        table_name: str,
        target_database: str,
        sql_dir: str,
        partition_num: int):
    
    target_table = table_name

    spark.sql(f"create database if not exists spark_catalog.{target_database}")

    # 创建相应的表
    sql_file = os.path.join(sql_dir, f"{target_table.lower()}.sql")
    with open(sql_file, "r") as f:
        sql_text = f.read()
    # 先把所有语句按分号切开
    sql_statements = sql_text.split(";")
    # 遍历每条语句执行
    for statement in sql_statements:
        statement = statement.strip()
        if statement:  # 忽略空行
            # 替换 -d 传进去的参数（SPARK_CATALOG, DB, PARTITION_NUM）
            statement = statement.replace("${SPARK_CATALOG}", "spark_catalog") \
                            .replace("${DB}", target_database) \
                            .replace("${PARTITION_NUM}", str(partition_num))
            spark.sql(statement)

def create_all_tables(
        spark: SparkSession,
        table_names: list,
        target_database_prefix: str,
        sql_dir: str,
        partition_nums: list):
    
    for partition_num in partition_nums:
        target_database = f"{target_database_prefix}_{partition_num}"
        for table_name in table_names:
            create_table(
                spark=spark,
                table_name=table_name,
                target_database=target_database,
                sql_dir=sql_dir,
                partition_num=partition_num
            )


if __name__ == "__main__":
    table_names = [
        "store_sales",
        "store_returns",
        "web_sales",
        "web_returns"
    ]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("create_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://master:9083") \
        .enableHiveSupport() \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "hdfs:///spark-events") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "8") \
        .getOrCreate()
    
    sql_dir = ""
    partition_nums = [1, 2, 4, 8, 16] 
    target_database_prefix = "test"
    for partition_num in partition_nums:
        target_database = f"{target_database_prefix}_{partition_num}"
        for table_name in table_names:
            create_table(
                table_name=table_name,
                target_database=target_database,
                sql_dir=sql_dir,
                partition_num=partition_num
            )
    print("All tables created successfully.")
    