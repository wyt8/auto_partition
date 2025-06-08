create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists store_sales;
CREATE TABLE store_sales(
      ss_sold_date_sk int
,     ss_sold_time_sk int
,     ss_item_sk int
,     ss_customer_sk int
,     ss_cdemo_sk int
,     ss_hdemo_sk int
,     ss_addr_sk int
,     ss_store_sk int
,     ss_promo_sk int
,     ss_ticket_number int
,     ss_quantity int
,     ss_wholesale_cost decimal(7,2)
,     ss_list_price decimal(7,2)
,     ss_sales_price decimal(7,2)
,     ss_ext_discount_amt decimal(7,2)
,     ss_ext_sales_price decimal(7,2)
,     ss_ext_wholesale_cost decimal(7,2)
,     ss_ext_list_price decimal(7,2)
,     ss_ext_tax decimal(7,2)
,     ss_coupon_amt decimal(7,2)
,     ss_net_paid decimal(7,2)
,     ss_net_paid_inc_tax decimal(7,2)
,     ss_net_profit decimal(7,2)  
)
USING iceberg
PARTITIONED BY (bucket(${PARTITION_NUM}, ss_sold_date_sk));
