create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists web_sales;
create table if not exists web_sales(
      ws_sold_date_sk int
,     ws_sold_time_sk int
,     ws_ship_date_sk int
,     ws_item_sk int
,     ws_bill_customer_sk int
,     ws_bill_cdemo_sk int
,     ws_bill_hdemo_sk int
,     ws_bill_addr_sk int
,     ws_ship_customer_sk int
,     ws_ship_cdemo_sk int
,     ws_ship_hdemo_sk int
,     ws_ship_addr_sk int
,     ws_web_page_sk int
,     ws_web_site_sk int
,     ws_ship_mode_sk int
,     ws_warehouse_sk int
,     ws_promo_sk int
,     ws_order_number int
,     ws_quantity int
,     ws_wholesale_cost decimal(7,2)
,     ws_list_price decimal(7,2)
,     ws_sales_price decimal(7,2)
,     ws_ext_discount_amt decimal(7,2)
,     ws_ext_sales_price decimal(7,2)
,     ws_ext_wholesale_cost decimal(7,2)
,     ws_ext_list_price decimal(7,2)
,     ws_ext_tax decimal(7,2)
,     ws_coupon_amt decimal(7,2)
,     ws_ext_ship_cost decimal(7,2)
,     ws_net_paid decimal(7,2)
,     ws_net_paid_inc_tax decimal(7,2)
,     ws_net_paid_inc_ship decimal(7,2)
,     ws_net_paid_inc_ship_tax decimal(7,2)
,     ws_net_profit decimal(7,2)
)
USING iceberg
PARTITIONED BY (bucket(${PARTITION_NUM}, ws_sold_date_sk));
