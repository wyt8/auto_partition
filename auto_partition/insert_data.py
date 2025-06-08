import time
from pyspark.sql import SparkSession


def insert_data(
        spark: SparkSession,
        table_name: str,
        source_database: str,
        target_database: str,
) -> int:
    
    source_table = table_name
    target_table = table_name

    total_start = time.time()

    spark.sql(f"insert into spark_catalog.{target_database}.{target_table} select * from spark_catalog.{source_database}.{source_table}")

    total_end = time.time()
    total_duration = total_end - total_start
    print(f"Insert time: {total_duration:.2f} seconds.")
    spark.stop()
    return total_duration

def insert_all_tables(
        spark: SparkSession,
        table_names: list,
        target_database_prefix: str,
        source_database: str,
        partition_nums: list,
):
    for partition_num in partition_nums:
        target_database = f"{target_database_prefix}_{partition_num}"
        for table_name in table_names:
            insert_data(
                spark=spark,
                table_name=table_name,
                source_database=source_database,
                target_database=target_database,
            )