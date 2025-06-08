create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists store_returns;
create table if not exists store_returns(
      sr_returned_date_sk int
,     sr_return_time_sk int
,     sr_item_sk int
,     sr_customer_sk int
,     sr_cdemo_sk int
,     sr_hdemo_sk int
,     sr_addr_sk int
,     sr_store_sk int
,     sr_reason_sk int
,     sr_ticket_number int
,     sr_return_quantity int
,     sr_return_amt decimal(7,2)
,     sr_return_tax decimal(7,2)
,     sr_return_amt_inc_tax decimal(7,2)
,     sr_fee decimal(7,2)
,     sr_return_ship_cost decimal(7,2)
,     sr_refunded_cash decimal(7,2)
,     sr_reversed_charge decimal(7,2)
,     sr_store_credit decimal(7,2)
,     sr_net_loss decimal(7,2)
)
USING iceberg
PARTITIONED BY (bucket(${PARTITION_NUM}, sr_returned_date_sk));
