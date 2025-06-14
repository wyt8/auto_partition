create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists web_returns;
create table if not exists web_returns(
      wr_returned_date_sk int
,     wr_returned_time_sk int
,     wr_item_sk int
,     wr_refunded_customer_sk int
,     wr_refunded_cdemo_sk int
,     wr_refunded_hdemo_sk int
,     wr_refunded_addr_sk int
,     wr_returning_customer_sk int
,     wr_returning_cdemo_sk int
,     wr_returning_hdemo_sk int
,     wr_returning_addr_sk int
,     wr_web_page_sk int
,     wr_reason_sk int
,     wr_order_number int
,     wr_return_quantity int
,     wr_return_amt decimal(7,2)
,     wr_return_tax decimal(7,2)
,     wr_return_amt_inc_tax decimal(7,2)
,     wr_fee decimal(7,2)
,     wr_return_ship_cost decimal(7,2)
,     wr_refunded_cash decimal(7,2)
,     wr_reversed_charge decimal(7,2)
,     wr_account_credit decimal(7,2)
,     wr_net_loss decimal(7,2) 
)
USING iceberg
PARTITIONED BY (bucket(${PARTITION_NUM}, wr_returned_date_sk));
