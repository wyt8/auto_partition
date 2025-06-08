create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists inventory;
create table if not exists inventory(
      inv_date_sk int
,     inv_item_sk int
,     inv_warehouse_sk int
,     inv_quantity_on_hand int
)
USING iceberg
PARTITIONED BY (bucket(${PARTITION_NUM}, inv_date_sk));
